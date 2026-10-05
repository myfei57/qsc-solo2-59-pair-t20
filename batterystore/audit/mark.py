"""操作边界索引。

一次并离网切换、一次消防联动会留下几十条流水，巡检关心的是"这次操作从哪到
哪、花了多久、碰了哪些组件"。这里按 ``op_id`` 把记录归并成操作区间，并支持
按组件与严重度过滤。

缺 ``op_id`` 的记录有两种归并口径，用策略常量区分：

* :data:`CYCLE_POLICY`（独立循环，默认口径）：每条无标识记录各自成一个独立
  的循环动作。分组只依赖记录自身字段与其在流水里的全局位置，任意顺序、任意
  并发写入、任意分段重放都得到同一组区间——这是落盘重建能和实时视图逐字节
  对齐的前提。
* :data:`ATTACH_POLICY`（并入最近一次操作）：无标识记录挂到它之前最近一次
  操作上。"最近一次"取决于写入到达顺序，两个线程交错写同时间戳的记录时
  结果会随调度变化，重放顺序一旦不同区间就对不上，因此只作为现场对照口径，
  不进落盘重建链路。

并发写入下两种口径给出的区间确实不同：交错到达的无标识记录在 ATTACH 下会被
拆进不同操作，CYCLE 下则始终是固定数量的独立区间。要让"重建结果必须和当时
实时看到的一致"，归并就必须与到达顺序无关，所以选 CYCLE_POLICY。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .record import AuditRecord, SEVERITIES

CYCLE_POLICY = "cycle"
ATTACH_POLICY = "attach"
POLICIES = (CYCLE_POLICY, ATTACH_POLICY)

# 严重度从低到高，区间取其中最高一档展示。
_SEVERITY_RANK = {name: index for index, name in enumerate(SEVERITIES)}


@dataclass
class OperationAccumulator:
    """一次操作区间的流式累加状态。

    所有字段都是无序聚合：同一批记录无论以什么到达顺序折入，输出完全相同，
    实时视图与落盘重建才能逐字节一致。
    """

    op_id: str
    first_ts: float
    last_ts: float
    count: int = 0
    components: set = field(default_factory=set)
    actions: dict = field(default_factory=dict)
    severities: dict = field(default_factory=dict)
    # (组件, 严重度) -> 条数，用于在整区间上精确计算过滤命中数。
    pairs: dict = field(default_factory=dict)
    marked: bool = True

    def add(self, record: AuditRecord) -> None:
        """把一条记录折进本区间。"""

        if record.ts < self.first_ts:
            self.first_ts = record.ts
        if record.ts > self.last_ts:
            self.last_ts = record.ts
        self.count += 1
        self.components.add(record.component)
        self.actions[record.action] = self.actions.get(record.action, 0) + 1
        self.severities[record.severity] = self.severities.get(record.severity, 0) + 1
        pair = (record.component, record.severity)
        self.pairs[pair] = self.pairs.get(pair, 0) + 1

    def as_interval(self) -> dict:
        """输出给控制台／自检用的区间字典。"""

        worst = max(self.severities, key=lambda name: _SEVERITY_RANK.get(name, -1))
        return {
            "op_id": self.op_id,
            "kind": "operation" if self.marked else "cycle",
            "first_ts": round(self.first_ts, 3),
            "last_ts": round(self.last_ts, 3),
            "span_s": round(self.last_ts - self.first_ts, 3),
            "count": self.count,
            "components": sorted(self.components),
            "actions": dict(sorted(self.actions.items())),
            "severities": dict(sorted(self.severities.items())),
            "max_severity": worst,
        }


def _cycle_key(seq: int) -> str:
    return f"cycle-{seq:012d}"


class OperationIndex:
    """流式操作归并器。

    记录按写入序号 ``seq`` 逐条折入；``seq`` 是该记录在整条流水里的全局位置，
    实时写入与落盘重放取同一个序号，CYCLE 口径的结果因此与来源无关。
    """

    def __init__(self, policy: str = CYCLE_POLICY) -> None:
        if policy not in POLICIES:
            raise ValueError(f"未知归并口径: {policy}")
        self._policy = policy
        self._buckets: dict[str, OperationAccumulator] = {}
        self._last_key = ""
        self._last_seq = -1

    @property
    def policy(self) -> str:
        return self._policy

    def fold(self, record: AuditRecord, seq: int) -> str:
        """折入一条记录，返回它落入的区间键。"""

        op_id = record.op_id()
        if op_id:
            key = op_id
        elif self._policy == ATTACH_POLICY and self._last_key:
            # 挂到写入序号最近的一次操作上；同序号的并发记录只能按到达
            # 顺序决定归属，这正是该口径不可重放的原因。
            key = self._last_key
        else:
            key = _cycle_key(seq)
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = OperationAccumulator(
                op_id=key, first_ts=record.ts, last_ts=record.ts, marked=bool(op_id)
            )
            self._buckets[key] = bucket
        bucket.add(record)
        if op_id and seq > self._last_seq:
            self._last_key, self._last_seq = key, seq
        return key

    def intervals(
        self,
        *,
        component: str | None = None,
        severity: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        """返回区间（最近结束的在前），可按组件与严重度过滤。

        过滤口径是"先筛记录、再归并"：区间只要含命中条件的记录就保留，
        起止时间仍是该次操作的完整跨度，``components`` 也仍是涉及的全部
        组件——否则区间会被过滤条件切成假的起止点。命中条数见 ``matched``。
        """

        result = []
        for bucket in self._buckets.values():
            matched = self._matched(bucket, component=component, severity=severity)
            if matched == 0:
                continue
            interval = bucket.as_interval()
            interval["matched"] = matched
            result.append(interval)
        result.sort(key=lambda item: (item["last_ts"], item["first_ts"], item["op_id"]), reverse=True)
        return result[: max(1, limit)]

    @staticmethod
    def _matched(
        bucket: OperationAccumulator, *, component: str | None, severity: str | None
    ) -> int:
        return sum(
            number
            for (name, level), number in bucket.pairs.items()
            if (component is None or name == component)
            and (severity is None or level == severity)
        )


def fold_operations(records, policy: str = CYCLE_POLICY) -> OperationIndex:
    """把一批记录按给定口径折成归并器。

    入参元素可以是 :class:`AuditRecord`（序号取其在批次中的位置，仅适合全量
    重放），也可以是 ``(record, seq)`` 元组（跨段合并时用全局序号）。
    """

    index = OperationIndex(policy)
    for position, item in enumerate(records):
        if isinstance(item, tuple):
            record, seq = item
        else:
            record, seq = item, position
        index.fold(record, int(seq))
    return index


def index_operations(
    records,
    limit: int = 20,
    *,
    component: str | None = None,
    severity: str | None = None,
    policy: str = CYCLE_POLICY,
) -> list[dict]:
    """按操作标识归并记录，返回最近若干次操作区间。

    ``records`` 元素为纯记录时按列表位置当流水序号；传入 ``(record, seq)``
    则按全局序号归并，保证跨段重放与实时一致。
    """

    return fold_operations(records, policy).intervals(
        component=component, severity=severity, limit=limit
    )


def compare_policies(records) -> dict:
    """对照两种口径在同一批记录上的区间，供巡检页面说明选型理由。"""

    return {
        "selected": CYCLE_POLICY,
        "reason": "cycle 口径只依赖记录自身，并发交错与分段重放都不改结果",
        "cycle": index_operations(records, policy=CYCLE_POLICY),
        "attach": index_operations(records, policy=ATTACH_POLICY),
    }
