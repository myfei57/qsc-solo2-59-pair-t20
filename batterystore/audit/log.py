"""运行审计。

``AuditLog`` 是事件总线的全量订阅者：组件只发布事件，日志把它们落成流水并
保留最近一段的内存副本。控制台读内存副本，跨重启回放读落盘流水；两份数据
都来自同一个 :class:`AuditRecord` 结构，字段口径一致。

操作区间的归并不走内存窗口，而是随写入逐条折进一个 :class:`OperationIndex`：
原始记录滚出内存窗口后，已经归并出的区间仍然完整保留——一次横跨滚段边界的
操作不会因为中间那批记录被删掉而丢掉前半截。归并器只按 CYCLE 口径工作，与
到达顺序无关；从落盘流水重建区间时用的是同一个纯归并函数和同一套全局序号，
所以重建结果必然与当时实时看到的结论逐字节一致。
"""

from __future__ import annotations

import threading

from ..clock import Clock
from ..eventbus import Event, EventBus
from ..store.journal import JsonlJournal
from .mark import CYCLE_POLICY, OperationIndex, compare_policies, index_operations
from .query import query_entries
from .record import AuditRecord
from .report import build_report
from .snapshot import AuditSnapshot
from .summary import summarize


class AuditLog:
    """运行记录：最近一段内存副本 + 分段落盘 + 全量操作区间索引。"""

    def __init__(self, clock: Clock, bus: EventBus, journal: JsonlJournal, memory_limit: int = 4096) -> None:
        self._clock = clock
        self._journal = journal
        self._entries: list[AuditRecord] = []
        self._memory_limit = max(1, memory_limit)
        # 区间索引的内存占用只与"操作次数"有关，与流水条数无关；原始记录
        # 仍只保留最近 memory_limit 条。
        self._live_index = OperationIndex(CYCLE_POLICY)
        self._lock = threading.RLock()
        self._rebuild_live_index()
        bus.subscribe("*", self._on_event)

    @property
    def journal(self) -> JsonlJournal:
        return self._journal

    def _on_event(self, event: Event) -> None:
        severity = "notice"
        payload = dict(event.payload)
        if "severity" in payload:
            severity = str(payload.pop("severity"))
        station = str(payload.pop("station", ""))
        message = str(payload.pop("message", ""))
        self.record(
            AuditRecord.create(
                self._clock.now(), station, event.component, event.action, severity, message, **payload
            )
        )

    def record(self, record: AuditRecord) -> dict:
        """记一条流水（内存 + 落盘），返回其字典形式。"""

        with self._lock:
            seq = self._journal.count()
            self._journal.append(record.as_dict())
            self._entries.append(record)
            if len(self._entries) > self._memory_limit:
                del self._entries[: len(self._entries) - self._memory_limit]
            self._live_index.fold(record, seq)
        return record.as_dict()

    def entries(self, limit: int = 100) -> list[dict]:
        """返回最近的记录（最新的在前）。"""

        with self._lock:
            selected = list(self._entries)[-limit:]
        return [entry.as_dict() for entry in reversed(selected)]

    def query(
        self,
        *,
        component: str | None = None,
        severity: str | None = None,
        action: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """按条件过滤记录。"""

        with self._lock:
            selected = list(self._entries)
        return query_entries(selected, component=component, severity=severity, action=action, limit=limit)

    def tail(self, limit: int = 20) -> list[dict]:
        """返回最近的记录（按时间升序）。"""

        with self._lock:
            selected = list(self._entries)[-limit:]
        return [entry.as_dict() for entry in selected]

    def alarms(self) -> list[dict]:
        """返回全部告警档记录。"""

        with self._lock:
            selected = [entry for entry in self._entries if entry.is_alarm()]
        return [entry.as_dict() for entry in selected]

    def records(self, limit: int = 200) -> list[AuditRecord]:
        """返回最近的记录对象（按时间升序），供时间线构造使用。"""

        with self._lock:
            return list(self._entries)[-limit:]

    def operations(
        self,
        limit: int = 20,
        *,
        component: str | None = None,
        severity: str | None = None,
        rebuilt: bool = False,
    ) -> list[dict]:
        """返回按操作标识归并的区间，可按组件与严重度过滤。

        默认读随写入维护的实时索引：内存窗口滚段之后区间照样完整。
        ``rebuilt=True`` 时从落盘流水全段重建，结果必须与实时口径一致，
        供跨重启恢复与自检对照使用。
        """

        if rebuilt:
            return self.rebuild_operations(limit=limit, component=component, severity=severity)
        with self._lock:
            index = self._live_index
            return index.intervals(component=component, severity=severity, limit=limit)

    def rebuild_operations(
        self,
        limit: int = 20,
        *,
        component: str | None = None,
        severity: str | None = None,
    ) -> list[dict]:
        """从全部落盘段重建操作区间（含内存中尚未滚段的记录）。

        归并函数、全局序号与实时路径完全相同：记录在流水里排第几，序号
        就是几。因此无论跨了多少个滚段、内存窗口多小，重建都不会少掉中间
        任何一段。
        """

        raws = self._journal.read_all()
        pairs = ((AuditRecord.from_dict(raw), seq) for seq, raw in enumerate(raws))
        return index_operations(
            pairs, limit=limit, component=component, severity=severity, policy=CYCLE_POLICY
        )

    def operation_policy_compare(self, limit: int = 20) -> dict:
        """对照两种无标识归并口径，说明为何落盘链路选独立循环。

        两种口径都在同一份全量流水（按落盘顺序）上计算，差异完全来自口径
        本身，而不是数据范围。
        """

        raws = self._journal.read_all()
        pairs = [(AuditRecord.from_dict(raw), seq) for seq, raw in enumerate(raws)]
        report = compare_policies(pairs)
        report["cycle"] = report["cycle"][: max(1, limit)]
        report["attach"] = report["attach"][: max(1, limit)]
        return report

    def _rebuild_live_index(self) -> None:
        """启动时把落盘流水全量折回实时索引，保证重启后区间连续。"""

        raws = self._journal.read_all()
        for seq, raw in enumerate(raws):
            self._live_index.fold(AuditRecord.from_dict(raw), seq)

    def summary(self) -> dict:
        """返回审计汇总计数。"""

        with self._lock:
            entries = list(self._entries)
        report = summarize(entries)
        report["journal_count"] = self._journal.count()
        report["active_segment"] = self._journal.active_segment()
        return report

    def report(self) -> dict:
        """返回带时间跨度的审计报告。"""

        with self._lock:
            entries = list(self._entries)
        return build_report(entries)

    def snapshot(self) -> AuditSnapshot:
        """返回审计状态快照。"""

        with self._lock:
            entries = list(self._entries)
            operations = self._live_index.intervals(limit=10)
        return AuditSnapshot(
            summary=summarize(entries),
            report=build_report(entries, recent=5),
            operations=operations,
            journal_count=self._journal.count(),
            active_segment=self._journal.active_segment(),
            segments=len(self._journal.segments()),
        )

    def replay_from_journal(self, limit: int = 100_000) -> list[dict]:
        """从落盘流水回放记录，供跨重启恢复使用。"""

        return self._journal.read_all()[-limit:]

    def replay_segment(self, index: int) -> list[dict]:
        """按段号回放一段流水。"""

        return self._journal.read_segment(index)
