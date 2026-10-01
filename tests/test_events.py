"""事件哈希链：篡改、乱序、重复必须给出可区分的原因。"""

from __future__ import annotations

import pytest

from ledger.errors import EventCorrupted, EventDuplicateError, EventOrderError
from ledger.events import (
    canonical_payload,
    decode_event,
    encode_event,
    make_event,
    verify_event,
)

TS0 = "2026-10-01T00:00:00+00:00"


def _chain(n: int):
    events = []
    prev = ""
    for i in range(1, n + 1):
        e = make_event(
            seq=i, event_type="transfer_committed", tenant_id="t1",
            payload={"i": i}, prev_hash=prev,
            timestamp=TS0, request_id=f"req-{i}",
        )
        events.append(e)
        prev = e.event_hash
    return events


def test_canonical_payload_is_stable():
    assert canonical_payload({"b": 1, "a": 2}) == '{"a":2,"b":1}'
    assert canonical_payload({"a": 2, "b": 1}) == '{"a":2,"b":1}'


def test_valid_chain_passes():
    events = _chain(3)
    prev = ""
    for i, e in enumerate(events, start=1):
        verify_event(e, expected_seq=i, prev_hash=prev)
        prev = e.event_hash


def test_payload_tamper_detected_as_corrupted():
    e1, e2 = _chain(2)
    bad = decode_event({**encode_event(e2), "payload": {"i": 999}})
    with pytest.raises(EventCorrupted, match="校验和不匹配"):
        verify_event(bad, expected_seq=2, prev_hash=e1.event_hash)


def test_hash_field_tamper_detected_as_corrupted():
    e1, e2 = _chain(2)
    raw = encode_event(e2)
    raw["event_hash"] = "0" * 64
    bad = decode_event(raw)
    with pytest.raises(EventCorrupted):
        verify_event(bad, expected_seq=2, prev_hash=e1.event_hash)


def test_old_seq_is_duplicate():
    e1, e2, _ = _chain(3)
    with pytest.raises(EventDuplicateError, match="重复事件"):
        verify_event(e1, expected_seq=2, prev_hash=e1.event_hash)


def test_seq_jump_is_out_of_order():
    _, e2, e3 = _chain(3)
    # 期望 2 却直接见到 3 -> 缺事件/乱序
    with pytest.raises(EventOrderError, match="跳跃"):
        verify_event(e3, expected_seq=2, prev_hash=e2.event_hash)


def test_broken_prev_hash_is_out_of_order():
    e1, e2 = _chain(2)
    with pytest.raises(EventOrderError, match="哈希链断裂"):
        verify_event(e2, expected_seq=2, prev_hash="deadbeef")


def test_decode_missing_field_is_corrupted():
    raw = encode_event(_chain(1)[0])
    del raw["event_hash"]
    with pytest.raises(EventCorrupted, match="缺少字段"):
        decode_event(raw)


def test_decode_bad_version_is_corrupted():
    raw = encode_event(_chain(1)[0])
    raw["v"] = 999
    with pytest.raises(EventCorrupted, match="格式版本"):
        decode_event(raw)
