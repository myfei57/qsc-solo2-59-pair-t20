"""运行审计。

``AuditLog`` 是事件总线的全量订阅者：组件只发布事件，日志把它们落成流水并
保留内存副本。控制台读内存副本，跨重启回放读落盘流水；两份数据都来自同一个
:class:`AuditRecord` 结构，字段口径一致。

内存只留最近一段（``memory_limit`` 条），更早的记录靠分段流水保留。操作区间
因此有两个读取入口，二者折叠同一段记录时结论必须一致：

* :meth:`operations` 折叠内存窗口（实时巡检看的就是这份）；
* :meth:`rebuild_operations` 从全部落盘流水重建。重建取“最近 N 条”时，与
  当时实时窗口对应的就是流水末尾同样的 N 条——归并是与顺序无关的纯折叠，
  日志滚段不会让跨段的一次操作丢掉中间那部分。

要保证滚段后仍能重建，落盘流水的保留范围必须覆盖要查询的重建窗口；流水只
追加、不在本模块内删除，跨段操作的每一条都还在原段落盘里。
"""

from __future__ import annotations

import threading

from ..clock import Clock
from ..eventbus import Event, EventBus
from ..store.journal import JsonlJournal
from .mark import index_operations
from .query import query_entries
from .record import AuditRecord
from .report import build_report
from .snapshot import AuditSnapshot
from .summary import summarize


class AuditLog:
    """运行记录：内存副本 + 分段落盘。"""

    def __init__(self, clock: Clock, bus: EventBus, journal: JsonlJournal, memory_limit: int = 4096) -> None:
        self._clock = clock
        self._journal = journal
        self._entries: list[AuditRecord] = []
        self._memory_limit = max(1, memory_limit)
        self._lock = threading.RLock()
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
        """记一条流水（内存 + 落盘），返回其字典形式。

        入库序号直接取落盘前的流水累计条数：序号即该记录在只追加流水中的行号，
        内存淘汰与跨段滚动都不改号，重建时还原出的序号与实时分配的完全相同。
        """

        with self._lock:
            numbered = record.with_seq(self._journal.count())
            self._entries.append(numbered)
            if len(self._entries) > self._memory_limit:
                del self._entries[: len(self._entries) - self._memory_limit]
            self._journal.append(numbered.as_dict())
        return numbered.as_dict()

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
        station: str | None = None,
    ) -> list[dict]:
        """返回内存窗口内按操作标识归并的区间（实时巡检口径）。"""

        with self._lock:
            selected = list(self._entries)
        return index_operations(
            selected, limit=limit, component=component, severity=severity, station=station
        )

    def rebuild_operations(
        self,
        limit: int = 20,
        *,
        window: int | None = None,
        component: str | None = None,
        severity: str | None = None,
        station: str | None = None,
    ) -> list[dict]:
        """从落盘流水重建操作区间。

        ``window`` 给定时只折叠最近 ``window`` 条，口径与“当时内存窗口等于
        window 条”实时看到的一致：二者折叠的是同样那一段记录（流水末尾 window
        条），归并与到达顺序无关，滚段也不会丢掉跨段操作的中间记录。``window``
        缺省折叠全部落盘流水。重建前先在锁内取一致的流水条数，避免读到追加到
        一半的窗口。
        """

        with self._lock:
            total = self._journal.count()
            raw = self._journal.read_all()
        if len(raw) > total:
            raw = raw[:total]
        if window is not None and window >= 0:
            raw = raw[-max(0, window):]
        records = [AuditRecord.from_dict(item) for item in raw]
        return index_operations(
            records, limit=limit, component=component, severity=severity, station=station
        )

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
        return AuditSnapshot(
            summary=summarize(entries),
            report=build_report(entries, recent=5),
            operations=index_operations(entries, limit=10),
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
