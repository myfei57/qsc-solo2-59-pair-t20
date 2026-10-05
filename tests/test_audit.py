"""审计记录、汇总、时间线与操作边界。"""

from __future__ import annotations

import threading
from pathlib import Path

from batterystore.audit.log import AuditLog
from batterystore.audit.mark import (
    ATTACH_POLICY,
    CYCLE_POLICY,
    index_operations,
)
from batterystore.audit.record import AuditRecord
from batterystore.audit.timeline import build_timeline
from batterystore.clock import ManualClock
from batterystore.eventbus import EventBus
from batterystore.store.journal import JsonlJournal


def _record(ts, component="grid", action="step", severity="notice", op_id=""):
    payload = {"op_id": op_id} if op_id else {}
    return AuditRecord.create(float(ts), "station1", component, action, severity, **payload)


def _sample_records():
    return [
        _record(1.0, "bms", "ready", op_id="op-A"),
        _record(1.5, "grid", "orphan-1"),
        _record(2.0, "pcs", "transfer", "alarm", "op-A"),
        _record(3.0, "bms", "orphan-2"),
        _record(4.0, "pcs", "transfer", op_id="op-B"),
    ]


def test_audit_record_roundtrip():
    record = AuditRecord.create(1.0, "station1", "pcs", "transfer", severity="bad", message="m", op_id="op-1")
    assert record.severity == "notice"
    assert record.is_alarm() is False
    assert record.op_id() == "op-1"
    restored = AuditRecord.from_dict(record.as_dict())
    assert restored == record
    assert AuditRecord.create(1.0, "s", "c", "a", severity="alarm").is_alarm() is True
    assert AuditRecord.create(1.0, "s", "c", "a").op_id() == ""


def test_operations_merge_by_op_id_into_intervals():
    intervals = index_operations(_sample_records())
    by_id = {item["op_id"]: item for item in intervals}
    # 两次有标识操作 + 两条无标识记录各自独立成循环，而不是并成一个 cycle 桶。
    assert by_id["op-A"]["kind"] == "operation"
    assert (by_id["op-A"]["first_ts"], by_id["op-A"]["last_ts"]) == (1.0, 2.0)
    assert by_id["op-A"]["span_s"] == 1.0
    assert by_id["op-A"]["count"] == 2
    assert by_id["op-A"]["components"] == ["bms", "pcs"]
    assert by_id["op-A"]["max_severity"] == "alarm"
    assert by_id["op-A"]["severities"] == {"alarm": 1, "notice": 1}
    assert by_id["op-B"]["count"] == 1
    cycles = [item for item in intervals if item["kind"] == "cycle"]
    assert len(cycles) == 2
    assert all(item["count"] == 1 for item in cycles)
    # 最近结束的排在前面；末尾无标识循环夹在两次操作之间。
    order = [item["op_id"] for item in intervals]
    assert order[0] == "op-B"
    assert order.index("op-A") > order.index("op-B")


def test_operations_filter_by_component_keeps_full_span():
    only_pcs = index_operations(_sample_records(), component="pcs")
    assert {item["op_id"] for item in only_pcs} == {"op-A", "op-B"}
    op_a = next(item for item in only_pcs if item["op_id"] == "op-A")
    # 命中过滤后起止跨度仍是整次操作，组件也仍是全部涉及组件。
    assert op_a["first_ts"] == 1.0 and op_a["last_ts"] == 2.0
    assert op_a["components"] == ["bms", "pcs"]
    assert op_a["matched"] == 1 and op_a["count"] == 2


def test_operations_filter_by_severity():
    alarms = index_operations(_sample_records(), severity="alarm")
    assert [item["op_id"] for item in alarms] == ["op-A"]
    assert alarms[0]["matched"] == 1 and alarms[0]["count"] == 2
    both = index_operations(_sample_records(), component="pcs", severity="alarm")
    assert [item["op_id"] for item in both] == ["op-A"]
    assert index_operations(_sample_records(), component="fire") == []


def test_cycle_policy_is_order_independent_with_fixed_sequence():
    records = _sample_records()
    pairs = [(record, seq) for seq, record in enumerate(records)]

    def signature(items):
        return sorted(
            (item["op_id"], item["first_ts"], item["last_ts"], item["count"], tuple(item["components"]))
            for item in items
        )

    def swap(pairs_list, i, j):
        swapped = list(pairs_list)
        swapped[i], swapped[j] = swapped[j], swapped[i]
        return swapped

    expected = signature(index_operations(pairs))
    # 并发写入等价于相邻记录到达顺序对调；序号跟着记录走，区间不变。
    for i in range(len(pairs) - 1):
        assert signature(index_operations(swap(pairs, i, i + 1))) == expected


def test_attach_policy_changes_with_concurrent_arrival_order():
    # 两个线程各发一条有标识记录和一条无标识记录，时间戳完全相同。
    thread_a = [_record(0.0, "bms", "ready", op_id="A"), _record(2.0, "grid", "orphan-a")]
    thread_b = [_record(0.5, "pcs", "ramp", op_id="B"), _record(2.0, "grid", "orphan-b")]

    def merge(picks):
        i = j = 0
        merged = []
        for pick in picks:
            if pick == 0:
                merged.append(thread_a[i])
                i += 1
            else:
                merged.append(thread_b[j])
                j += 1
        return merged

    first = {item["op_id"]: item for item in index_operations(merge([0, 1, 0, 1]), policy=ATTACH_POLICY)}
    second = {item["op_id"]: item for item in index_operations(merge([1, 0, 1, 0]), policy=ATTACH_POLICY)}
    # 交错顺序不同，无标识记录挂到的"最近一次操作"就不同，区间归属随调度变化。
    assert first["A"]["count"] == 1 and first["B"]["count"] == 3
    assert second["A"]["count"] == 3 and second["B"]["count"] == 1
    # 同样的交错在 cycle 口径下始终是 4 个区间且归属不变。
    for picks in ([0, 1, 0, 1], [1, 0, 1, 0]):
        merged = merge(picks)
        cycles = index_operations(merged, policy=CYCLE_POLICY)
        marked = {item["op_id"]: item for item in cycles if item["kind"] == "operation"}
        assert marked["A"]["count"] == 1 and marked["B"]["count"] == 1
        assert len([item for item in cycles if item["kind"] == "cycle"]) == 2


def _build_rolling_log(root: Path, segment_limit: int = 4, memory_limit: int = 6) -> AuditLog:
    log = AuditLog(ManualClock(), EventBus(), JsonlJournal(str(root), "audit", segment_limit), memory_limit)
    # 一次操作的头两条落在很早的段里，最后一条隔了十条循环记录才到，
    # 横跨多个滚段且超出内存窗口。
    log.record(_record(1.0, "bms", "ready", op_id="op-1"))
    log.record(_record(1.5, "pcs", "ramp", op_id="op-1"))
    for index in range(10):
        log.record(_record(2.0 + index, "grid", f"cycle-{index}"))
    log.record(_record(20.0, "pcs", "transfer", "alarm", "op-1"))
    log.record(_record(21.0, "fire", "drill", op_id="op-2"))
    return log


def test_rebuild_matches_live_after_segments_roll_out_of_window(tmp_path):
    log = _build_rolling_log(tmp_path)
    assert log.journal.count() == 14
    assert len(log.journal.segments()) == 4
    # 内存窗口只留 6 条，op-1 的开头早就滚出去了。
    assert len(log.records(limit=100)) == 6

    live = log.operations(limit=50)
    rebuilt = log.rebuild_operations(limit=50)
    assert rebuilt == live

    by_id = {item["op_id"]: item for item in rebuilt}
    assert (by_id["op-1"]["first_ts"], by_id["op-1"]["last_ts"], by_id["op-1"]["count"]) == (
        1.0,
        20.0,
        3,
    )
    assert by_id["op-1"]["components"] == ["bms", "pcs"]
    assert len([item for item in rebuilt if item["kind"] == "cycle"]) == 10
    # 过滤口径在重建路径上同样成立。
    assert [item["op_id"] for item in log.operations(severity="alarm", rebuilt=True)] == ["op-1"]


def test_live_index_survives_restart_with_same_intervals(tmp_path):
    log = _build_rolling_log(tmp_path)
    expected = log.rebuild_operations(limit=50)

    reopened = AuditLog(
        ManualClock(),
        EventBus(),
        JsonlJournal(str(tmp_path), "audit", 4),
        memory_limit=6,
    )
    assert reopened.journal.count() == 14
    assert reopened.operations(limit=50) == expected

    # 重启后继续写入，序号与区间都接着旧流水走。
    reopened.record(_record(30.0, "grid", "step", op_id="op-3"))
    again = reopened.rebuild_operations(limit=50)
    by_id = {item["op_id"]: item for item in again}
    assert by_id["op-3"]["first_ts"] == 30.0
    assert by_id["op-1"]["first_ts"] == 1.0
    assert len([item for item in again if item["kind"] == "cycle"]) == 10


def test_policy_compare_shows_divergence(tmp_path):
    log = _build_rolling_log(tmp_path)
    report = log.operation_policy_compare(limit=50)
    assert report["selected"] == CYCLE_POLICY
    cycle_ids = {item["op_id"] for item in report["cycle"]}
    attach_ids = {item["op_id"] for item in report["attach"]}
    # 无标识记录在 attach 下被并进 op-1，独立循环区间在该口径里不存在。
    assert "op-1" in attach_ids and not (attach_ids & {key for key in cycle_ids if key.startswith("cycle-")})
    assert len([item for item in report["cycle"] if item["kind"] == "cycle"]) == 10


def test_concurrent_writes_still_rebuild_identically(tmp_path):
    log = AuditLog(ManualClock(), EventBus(), JsonlJournal(str(tmp_path), "audit", 8), memory_limit=16)

    def writer(start):
        for index in range(20):
            op_id = f"op-{start}" if index % 5 else ""
            log.record(_record(index, "grid", "step", op_id=op_id))

    threads = [threading.Thread(target=writer, args=(mark,)) for mark in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # 无论写入时线程如何交错，落盘顺序确定后，实时与重建必须逐字节一致。
    assert log.operations(limit=100) == log.rebuild_operations(limit=100)


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
