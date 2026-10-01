"""事件回放：确定性、检查点、崩溃恢复、损坏/乱序/重复识别。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ledger.clock import SystemClock
from ledger.errors import EventCorrupted, EventDuplicateError, EventOrderError
from ledger.service import LedgerService
from ledger.storage import Storage


def _seed(service) -> None:
    service.create_tenant("t1", "租户甲", "key-1", currencies=("CNY",))
    service.open_account("t1", "a", "CNY", initial_balance=1000)
    service.open_account("t1", "b", "CNY", initial_balance=0)
    service.transfer(tenant_id="t1", idempotency_key="k1", request_id="req-1",
                     from_account="a", to_account="b", amount=300, fee=10,
                     currency="CNY")
    service.transfer(tenant_id="t1", idempotency_key="k2", request_id="req-2",
                     from_account="a", to_account="b", amount=50, fee=5,
                     currency="CNY")


def test_full_replay_is_deterministic(service):
    _seed(service)
    s1 = service.replay("t1", from_checkpoint=False)
    s2 = service.replay("t1", from_checkpoint=False)
    assert s1["balances"] == s2["balances"]
    assert s1["head_hash"] == s2["head_hash"]
    assert s1["balances"] == {"a": 635, "b": 350, "_fee_CNY": 15}
    assert s1["events_replayed"] == 5


def test_replay_rebuilds_state_in_new_process(tmp_home):
    """模拟进程重启：丢弃内存状态，用全新服务从磁盘重建。"""
    svc1 = LedgerService(Storage(str(tmp_home)), SystemClock())
    _seed(svc1)
    svc1.save_checkpoint("t1")

    svc2 = LedgerService(Storage(str(tmp_home)), SystemClock())
    bal = svc2.get_balance("t1", "a")
    assert bal.balance == 635 and bal.version == 5
    # 重启后幂等仍有效：重发不重复入账，返回同一笔
    r = svc2.transfer(
        tenant_id="t1", idempotency_key="k1", request_id="req-retry-after-restart",
        from_account="a", to_account="b", amount=300, fee=10, currency="CNY",
    )
    assert r.replayed is True
    assert svc2.get_balance("t1", "a").balance == 635


def test_checkpoint_then_incremental_replay(service):
    _seed(service)
    service.save_checkpoint("t1")  # seq=5
    service.transfer(tenant_id="t1", idempotency_key="k3", request_id="req-3",
                     from_account="b", to_account="a", amount=100, fee=1,
                     currency="CNY")
    summary = service.replay("t1", from_checkpoint=True)
    assert summary["checkpoint_seq"] == 5
    assert summary["events_replayed"] == 1
    assert summary["head_seq"] == 6
    # a:635+100=735, b:350-101=249, fee:15+1=16
    assert summary["balances"] == {"a": 735, "b": 249, "_fee_CNY": 16}


def test_stale_checkpoint_detected(service, tmp_home):
    """检查点之后若被旧日志覆盖（锚点不一致），必须报错而不是静默接受。"""
    _seed(service)
    service.save_checkpoint("t1")
    log = Path(tmp_home) / "t1" / "events.log"
    lines = log.read_text(encoding="utf-8").splitlines()
    # 删除最后一条事件，使检查点 seq=4 失去锚点
    log.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")
    with pytest.raises(EventCorrupted, match="检查点"):
        service.replay("t1", from_checkpoint=True)


def test_crash_between_event_and_idempotency_record(service, tmp_home, caplog):
    """故障回放核心场景：事件已落盘、幂等记录未及写入。

    手工制造该状态：先正常提交，再删掉幂等记录文件，然后回放，
    必须自动补建幂等记录，且重试返回与原请求一致的结果。
    """
    _seed(service)
    idem_files = list((Path(tmp_home) / "t1" / "idem").iterdir())
    k1_file = next(p for p in idem_files if "k1" in p.name)
    original = json.loads(k1_file.read_text(encoding="utf-8"))
    k1_file.unlink()

    summary = service.replay("t1", from_checkpoint=False)
    assert summary["head_seq"] == 5
    # 补建后重试：不重复入账
    r = service.transfer(
        tenant_id="t1", idempotency_key="k1", request_id="req-crash-retry",
        from_account="a", to_account="b", amount=300, fee=10, currency="CNY",
    )
    assert r.replayed is True
    assert r.transfer_id == original["response"]["transfer"]["transfer_id"]
    assert service.get_balance("t1", "a").balance == 635
    assert any("replay_repair_idempotency" in rec.message
               for rec in caplog.records)


def _write_raw_event(tmp_home: Path, raw: dict) -> None:
    with open(Path(tmp_home) / "t1" / "events.log", "a", encoding="utf-8") as fh:
        fh.write(json.dumps(raw, ensure_ascii=False) + "\n")


def test_replay_detects_corrupted_payload(service, tmp_home):
    _seed(service)
    events = service.list_events("t1")
    last = events[-1]
    from ledger.events import encode_event

    raw = encode_event(last)
    raw["payload"] = {**raw["payload"], "amount": 999999}  # 篡改但不改 hash
    # 直接替换最后一行
    log = Path(tmp_home) / "t1" / "events.log"
    lines = log.read_text(encoding="utf-8").splitlines()
    lines[-1] = json.dumps(raw, ensure_ascii=False)
    log.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(EventCorrupted, match="校验和不匹配"):
        service.replay("t1", from_checkpoint=False)


def test_replay_detects_duplicate_event(service, tmp_home):
    _seed(service)
    events = service.list_events("t1")
    # 把第 3 条事件再追加到末尾 -> 序号回退 -> 重复
    dup = events[2]
    from ledger.events import encode_event

    _write_raw_event(tmp_home, encode_event(dup))
    with pytest.raises(EventDuplicateError, match="重复事件"):
        service.replay("t1", from_checkpoint=False)


def test_replay_detects_out_of_order_event(service, tmp_home):
    """删掉 seq=3 后把 seq=4 留下 -> 期望 3 却见到 4 -> 乱序/缺失。"""
    _seed(service)
    log = Path(tmp_home) / "t1" / "events.log"
    lines = log.read_text(encoding="utf-8").splitlines()
    del lines[2]  # 删除 seq=3（开户 b）
    log.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(EventOrderError, match="跳跃"):
        service.replay("t1", from_checkpoint=False)


def test_replay_detects_garbage_line(service, tmp_home):
    _seed(service)
    with open(Path(tmp_home) / "t1" / "events.log", "a", encoding="utf-8") as fh:
        fh.write("{这不是合法JSON\n")
    with pytest.raises(EventCorrupted, match="不是合法 JSON"):
        service.replay("t1", from_checkpoint=False)


def test_verify_log_intact(service):
    _seed(service)
    result = service.verify_log("t1")
    assert result["intact"] is True and result["events"] == 5
