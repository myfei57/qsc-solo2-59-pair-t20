"""操作边界索引。

一次并离网切换、一次消防联动会留下十几条流水，巡检关心的是“这次操作从哪到
哪、花了多久、涉及哪些组件、最高到什么严重度”。这里把记录归并成操作区间。

归并口径（两种里固定选第一种）：

* **口径 A——独立循环动作**：有 ``op_id`` 的记录按标识归并；没有 ``op_id``
  的记录每条单独成为一个“循环动作”点区间。这是本模块采用的口径。
* 口径 B——并入最近一次操作：把无标识记录挂到当前最近的操作上。本模块不采用。

并发写入时两种口径给出的区间不同，必须二选一。选 A 的原因是：区间折叠只依赖
每条记录自身的字段，结果是记录集合的纯函数、与到达顺序无关；而 B 的“最近一次
操作”取决于到达时刻，并发交错时同一条无标识记录可能挂到不同操作上，落盘重放
顺序又与实时不同，会导致重建结论和实时看到的不一致。A 还满足结合律，实时折叠
与从落盘流水重建跑同一段记录必然得到同一批区间。

为保证同时间戳也能确定排序，无标识记录用入库序号 ``seq`` 单独编号；区间的
组件／严重度过滤在折叠完成之后按整条区间判定，只决定哪些区间出现，不会把一次
操作的起止时间裁短。
"""

from __future__ import annotations

from .record import SEVERITIES, AuditRecord

OPERATION = "operation"
CYCLE = "cycle"


def _bucket_key(record: AuditRecord, position: int) -> tuple[str, str, str]:
    """返回 (归并键, 区间类型, 对外操作标识)；无标识记录单独成键。"""

    op_id = record.op_id()
    if op_id:
        return op_id, OPERATION, op_id
    # 生产路径 seq 一定已分配；直接构造、未经审计日志的记录退回用遍历位置兜底。
    token = record.seq if record.seq >= 0 else f"anon-{position}"
    return f"{CYCLE}:{token}", CYCLE, ""


def index_operations(
    records: list[AuditRecord],
    *,
    limit: int = 20,
    component: str | None = None,
    severity: str | None = None,
    station: str | None = None,
) -> list[dict]:
    """按操作标识归并记录，返回最近若干次操作区间。

    ``component`` / ``severity`` / ``station`` 都按整条区间过滤：区间只要涉及
    该组件（或出现过该严重度、涉及该电站）就保留，区间边界仍是完整操作的边界。
    """

    buckets: dict[str, dict] = {}
    for position, record in enumerate(records):
        key, kind, op_id = _bucket_key(record, position)
        bucket = buckets.get(key)
        if bucket is None:
            bucket = {
                "key": key,
                "kind": kind,
                "op_id": op_id,
                "first_ts": record.ts,
                "last_ts": record.ts,
                "first_seq": record.seq,
                "last_seq": record.seq,
                "count": 0,
                "components": set(),
                "stations": set(),
                "actions": {},
                "severity_rank": -1,
                "severities": {},
            }
            buckets[key] = bucket
        else:
            bucket["first_ts"] = min(bucket["first_ts"], record.ts)
            bucket["last_ts"] = max(bucket["last_ts"], record.ts)
            bucket["first_seq"] = min(bucket["first_seq"], record.seq)
            bucket["last_seq"] = max(bucket["last_seq"], record.seq)
            bucket["severity_rank"] = max(bucket["severity_rank"], record.severity_rank())
        bucket["count"] += 1
        bucket["components"].add(record.component)
        bucket["stations"].add(record.station)
        bucket["actions"][record.action] = bucket["actions"].get(record.action, 0) + 1
        bucket["severities"][record.severity] = bucket["severities"].get(record.severity, 0) + 1
        bucket["severity_rank"] = max(bucket["severity_rank"], record.severity_rank())

    kept = [
        item
        for item in buckets.values()
        if (component is None or component in item["components"])
        and (severity is None or item["severities"].get(severity, 0) > 0)
        and (station is None or station in item["stations"])
    ]
    # 结束时间优先、入库序号兜底：即使并发导致时间戳相同也给出确定顺序。
    kept.sort(key=lambda item: (item["last_ts"], item["last_seq"]), reverse=True)
    return [_serialize(item) for item in kept[:limit]]


def _serialize(item: dict) -> dict:
    rank = item["severity_rank"] if item["severity_rank"] >= 0 else 0
    return {
        "kind": item["kind"],
        "op_id": item["op_id"],
        "first_ts": round(item["first_ts"], 3),
        "last_ts": round(item["last_ts"], 3),
        "span_s": round(item["last_ts"] - item["first_ts"], 3),
        "count": item["count"],
        "components": sorted(item["components"]),
        "stations": sorted(item["stations"]),
        "actions": sorted(item["actions"]),
        "action_counts": dict(sorted(item["actions"].items())),
        "severities": {name: item["severities"][name] for name in SEVERITIES if name in item["severities"]},
        "max_severity": SEVERITIES[rank],
    }
