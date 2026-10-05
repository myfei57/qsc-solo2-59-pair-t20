"""审计记录。

一条记录固定携带时间、电站名、组件、动作、严重度与说明，附带可选的上下文
字段。严重度分三档：``notice``（正常动作）、``warning``（可恢复异常）、
``alarm``（联锁拒绝、保护动作或消防联动），控制台的告警页只取后两档。

每条记录还带一个入库序号 ``seq``：并发写入时多条记录的 ``ts`` 可能完全相同，
归并区间需要一个与到达无关的全局总序来给无标识记录单独编号、给同时间戳决胜，
因此序号在落盘前由审计日志统一分配并一起持久化，重建时沿用同一序号。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

SEVERITIES = ("notice", "warning", "alarm")
SEVERITY_RANK = {name: rank for rank, name in enumerate(SEVERITIES)}


@dataclass(frozen=True)
class AuditRecord:
    """一条审计流水。"""

    ts: float
    station: str
    component: str
    action: str
    severity: str
    message: str
    payload: dict = field(default_factory=dict)
    seq: int = -1

    @classmethod
    def create(
        cls,
        ts: float,
        station: str,
        component: str,
        action: str,
        severity: str = "notice",
        message: str = "",
        **payload: object,
    ) -> "AuditRecord":
        if severity not in SEVERITIES:
            severity = "notice"
        return cls(ts, station, component, action, severity, message, dict(payload))

    @classmethod
    def from_dict(cls, raw: dict) -> "AuditRecord":
        """从落盘字段还原一条记录。"""

        return cls(
            ts=float(raw.get("ts", 0.0)),
            station=str(raw.get("station", "")),
            component=str(raw.get("component", "")),
            action=str(raw.get("action", "")),
            severity=str(raw.get("severity", "notice")),
            message=str(raw.get("message", "")),
            payload=dict(raw.get("payload", {})),
            seq=int(raw.get("seq", -1)),
        )

    def as_dict(self) -> dict:
        return {
            "ts": round(self.ts, 3),
            "station": self.station,
            "component": self.component,
            "action": self.action,
            "severity": self.severity,
            "message": self.message,
            "payload": self.payload,
            "seq": self.seq,
        }

    def with_seq(self, seq: int) -> "AuditRecord":
        """返回分配了入库序号后的同值记录。"""

        return replace(self, seq=seq)

    def is_alarm(self) -> bool:
        """是否属于告警档（warning / alarm）。"""

        return self.severity in ("warning", "alarm")

    def severity_rank(self) -> int:
        """严重度序号，越大越严重，用于求一次操作的最高严重度。"""

        return SEVERITY_RANK.get(self.severity, 0)

    def op_id(self) -> str:
        """返回该记录所属操作的标识，没有则返回空串。"""

        value = self.payload.get("op_id", "")
        return str(value) if value else ""
