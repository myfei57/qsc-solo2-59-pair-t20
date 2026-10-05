"""审计记录、汇总、时间线与操作边界。"""

from __future__ import annotations

from batterystore.audit.mark import index_operations
from batterystore.audit.record import AuditRecord
from batterystore.audit.timeline import build_timeline


def test_audit_record_roundtrip():
    record = AuditRecord.create(1.0, "station1", "pcs", "transfer", severity="bad", message="m", op_id="op-1")
    assert record.severity == "notice"
    assert record.is_alarm() is False
    assert record.op_id() == "op-1"
    restored = AuditRecord.from_dict(record.as_dict())
    assert restored == record
    assert AuditRecord.create(1.0, "s", "c", "a", severity="alarm").is_alarm() is True
    assert AuditRecord.create(1.0, "s", "c", "a").op_id() == ""


def test_audit_log_records_and_queries(platform):
    platform.start()
    platform.transfer_grid_tie()
    summary = platform.audit.summary()
    assert summary["total"] > 0
    assert summary["journal_count"] >= summary["total"]
    entries = platform.audit.query(component="pcs", limit=50)
    assert entries
    assert all(entry["component"] == "pcs" for entry in entries)
    assert platform.audit.tail(limit=1)
    assert platform.audit.entries(limit=1)
    assert platform.audit.report()["span_s"] >= 0.0
    assert platform.audit.operations(limit=5)
    assert platform.audit.replay_segment(0)
    assert platform.audit.replay_from_journal(limit=5)


def test_audit_snapshot_and_timeline(platform):
    platform.start()
    platform.report_fault("rk-01", "cell_over_temp")
    platform.report_fault("rk-01", "cell_over_temp")
    snapshot = platform.audit_snapshot()
    assert snapshot["journal_count"] > 0
    assert snapshot["summary"]["alarms"] >= 1
    assert snapshot["segments"] >= 1
    assert snapshot["operations"]
    timeline = build_timeline(platform.audit.records(limit=50), station="station1")
    assert timeline
    assert timeline[0]["ts"] <= timeline[-1]["ts"]
    assert build_timeline(platform.audit.records(limit=50), station="other") == []


def test_alarms_only_contain_warning_and_alarm(platform):
    platform.start()
    platform.report_fault("rk-01", "cell_over_temp")
    platform.report_fault("rk-01", "cell_over_temp")
    alarms = platform.audit.alarms()
    assert alarms
    assert {entry["severity"] for entry in alarms} <= {"warning", "alarm"}


def _rec(ts, component, action, *, severity="notice", op_id="", seq=-1):
    record = AuditRecord.create(
        ts, "station1", component, action, severity=severity, op_id=op_id
    )
    return record if seq < 0 else record.with_seq(seq)


def test_untagged_records_become_independent_cycles():
    records = [
        _rec(1.0, "pcs", "transfer", op_id="op-1", seq=0),
        _rec(2.0, "grid", "tie_check", op_id="op-1", seq=1),
        _rec(3.0, "bms", "heartbeat", seq=2),
        _rec(4.0, "temp", "sample", seq=3),
    ]
    intervals = index_operations(records, limit=10)
    assert [item["kind"] for item in intervals] == ["cycle", "cycle", "operation"]
    op = next(item for item in intervals if item["kind"] == "operation")
    assert op["op_id"] == "op-1"
    assert (op["first_ts"], op["last_ts"], op["count"]) == (1.0, 2.0, 2)
    assert op["components"] == ["grid", "pcs"]
    cycles = [item for item in intervals if item["kind"] == "cycle"]
    assert all(item["op_id"] == "" and item["count"] == 1 for item in cycles)
    assert {item["components"][0] for item in cycles} == {"bms", "temp"}


def test_operation_intervals_carry_severity_and_can_be_filtered():
    records = [
        _rec(1.0, "bms", "ready", op_id="op-1", seq=0),
        _rec(2.0, "pcs", "rejected", op_id="op-1", severity="alarm", seq=1),
        _rec(3.0, "grid", "sample", seq=2),
    ]
    op = index_operations(records)[-1]
    assert op["max_severity"] == "alarm"
    assert op["severities"] == {"notice": 1, "alarm": 1}
    # 组件过滤按整条区间判定，不裁短边界。
    only_bms = index_operations(records, component="bms")
    assert [item["op_id"] for item in only_bms] == ["op-1"]
    assert only_bms[0]["first_ts"] == 1.0 and only_bms[0]["last_ts"] == 2.0
    # 严重度过滤保留出现过该档的完整操作区间。
    with_alarm = index_operations(records, severity="alarm")
    assert [item["op_id"] for item in with_alarm] == ["op-1"]
    assert index_operations(records, severity="warning") == []
    # 与操作无关的组件不会把无关节区带出来。
    assert index_operations(records, component="temp") == []


def test_grouping_is_independent_of_arrival_order_under_concurrency():
    base = [
        _rec(1.0, "pcs", "a", op_id="op-A", seq=0),
        _rec(1.0, "bms", "b", op_id="op-B", seq=1),
        _rec(1.0, "grid", "c", seq=2),
        _rec(1.0, "pcs", "d", op_id="op-A", seq=3),
        _rec(1.0, "temp", "e", seq=4),
        _rec(1.0, "bms", "f", op_id="op-B", seq=5),
    ]
    forward = index_operations(base, limit=20)
    shuffled = index_operations(list(reversed(base)), limit=20)
    # 区间是记录集合的纯函数：并发交错改变到达顺序，结论完全一致。
    assert forward == shuffled
    assert sorted((item["op_id"], item["count"], tuple(item["components"])) for item in forward) == [
        ("", 1, ("grid",)),
        ("", 1, ("temp",)),
        ("op-A", 2, ("pcs",)),
        ("op-B", 2, ("bms",)),
    ]


def test_attaching_to_nearest_operation_depends_on_order():
    # 对照实验（不落生产代码）：口径 B 下同一条无标识记录因到达顺序归属不同操作，
    # 与实时/重建必须一致的要求冲突，这正是选择口径 A 的原因。
    def attach_policy_b(ordered):
        buckets = {}
        latest = None
        for record in ordered:
            op_id = record.op_id() or latest
            latest = record.op_id() or latest
            bucket = buckets.setdefault(op_id, {"count": 0, "components": set()})
            bucket["count"] += 1
            bucket["components"].add(record.component)
        return {key: (value["count"], tuple(sorted(value["components"]))) for key, value in buckets.items()}

    records = [
        _rec(1.0, "pcs", "a", op_id="op-A", seq=0),
        _rec(2.0, "grid", "loose", seq=1),
        _rec(3.0, "bms", "b", op_id="op-B", seq=2),
    ]
    assert attach_policy_b(records) != attach_policy_b(list(reversed(records)))


def test_rebuild_matches_live_window_after_memory_eviction(platform):
    window = 8
    platform.audit._memory_limit = window
    # seq 由审计日志按落盘行号分配，这里不显式传。
    for index in range(40):
        platform.audit.record(_rec(100.0 + index, "pcs", f"step-{index % 3}", op_id="op-long"))
    # 内存只剩最近 window 条：实时窗口
    live = platform.audit.operations(limit=20)
    # 从落盘流水重建“当时等于 window 条的实时窗口”：同一批区间。
    rebuilt = platform.audit.rebuild_operations(limit=20, window=window)
    assert live == rebuilt
    # 全量重建能拿到内存里早已淘汰的最早记录，且仍是完整操作区间。
    full = platform.audit.rebuild_operations(limit=20)
    assert len(full) == 1
    assert full[0]["count"] == 40
    assert full[0]["first_ts"] == 100.0
    assert full[0]["last_ts"] == 139.0


def test_segment_rollover_does_not_split_a_spanning_operation(platform):
    journal = platform.audit.journal
    # segment_limit=512：让一次操作的记录跨过 0/1 段的边界。
    platform.audit.record(_rec(0.0, "pcs", "before", op_id="op-span"))
    for index in range(1, 514):
        platform.audit.record(_rec(index, "bms", f"cycle-{index}"))
    platform.audit.record(_rec(600.0, "grid", "after", op_id="op-span"))
    assert len(journal.segments()) >= 2
    rebuilt = platform.audit.rebuild_operations(limit=1000)
    span = next(item for item in rebuilt if item["op_id"] == "op-span")
    assert span["count"] == 2
    assert span["first_ts"] == 0.0
    assert span["last_ts"] == 600.0
    assert span["components"] == ["grid", "pcs"]


def test_seq_is_journal_row_and_persisted(platform):
    platform.start()
    first = platform.audit.record(_rec(1.0, "pcs", "x"))
    second = platform.audit.record(_rec(2.0, "pcs", "y"))
    assert first["seq"] + 1 == second["seq"]
    restored = platform.audit.replay_from_journal(limit=10)
    assert [row["seq"] for row in restored[-2:]] == [first["seq"], second["seq"]]


def test_operations_endpoint_filters_and_rebuilds(platform):
    import json
    import urllib.request

    from batterystore.console.server import ConsoleServer

    platform.start()
    platform.transfer_grid_tie()
    server = ConsoleServer(platform, port=0)
    base_url = server.start()
    try:
        with urllib.request.urlopen(f"{base_url}/api/audit/operations?component=pcs", timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
        assert payload["source"] == "memory"
        assert payload["operations"]
        assert all("pcs" in item["components"] for item in payload["operations"])
        with urllib.request.urlopen(f"{base_url}/api/audit/operations?source=rebuild&component=pcs", timeout=5) as response:
            rebuilt = json.loads(response.read().decode("utf-8"))
        assert rebuilt["source"] == "rebuild"
        assert rebuilt["operations"] == payload["operations"]
    finally:
        server.stop()
