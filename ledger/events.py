"""事件编解码、哈希链与校验。

事件在租户内形成一条严格追加的哈希链：
  hash_n = sha256(v | prev_hash | seq | event_id | type | tenant |
                  request_id | idem_key | timestamp | canonical(payload))
任何对载荷的篡改、乱序插入、重复都会在 verify_event 阶段被识别，
并区分为 EventCorrupted / EventOrderError / EventDuplicateError。
"""

from __future__ import annotations

import hashlib
import json

from .errors import EventCorrupted, EventDuplicateError, EventOrderError
from .models import Event

# 本格式的版本号，写进每条事件，便于未来演进。
SCHEMA_VERSION = 1

_REQUIRED_FIELDS = (
    "seq",
    "event_id",
    "event_type",
    "tenant_id",
    "request_id",
    "idempotency_key",
    "timestamp",
    "payload",
    "prev_hash",
    "event_hash",
    "v",
)


def canonical_payload(payload: dict) -> str:
    """事件载荷的规范化 JSON（排序键、无空白），用于哈希与校验和。"""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_event_hash(
    *,
    prev_hash: str,
    seq: int,
    event_id: str,
    event_type: str,
    tenant_id: str,
    request_id: str | None,
    idempotency_key: str | None,
    timestamp: str,
    payload: dict,
) -> str:
    """计算单条事件的哈希（sha256 十六进制）。"""
    material = "|".join(
        [
            str(SCHEMA_VERSION),
            prev_hash,
            str(seq),
            event_id,
            event_type,
            tenant_id,
            request_id or "",
            idempotency_key or "",
            timestamp,
            canonical_payload(payload),
        ]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def make_event(
    *,
    seq: int,
    event_type: str,
    tenant_id: str,
    payload: dict,
    prev_hash: str,
    timestamp: str,
    request_id: str | None = None,
    idempotency_key: str | None = None,
    event_id: str | None = None,
) -> Event:
    """构造一条哈希自洽的事件。"""
    from .ids import new_event_id

    eid = event_id or new_event_id()
    event_hash = compute_event_hash(
        prev_hash=prev_hash,
        seq=seq,
        event_id=eid,
        event_type=event_type,
        tenant_id=tenant_id,
        request_id=request_id,
        idempotency_key=idempotency_key,
        timestamp=timestamp,
        payload=payload,
    )
    return Event(
        seq=seq,
        event_id=eid,
        event_type=event_type,
        tenant_id=tenant_id,
        request_id=request_id,
        idempotency_key=idempotency_key,
        timestamp=timestamp,
        payload=payload,
        prev_hash=prev_hash,
        event_hash=event_hash,
    )


def encode_event(event: Event) -> dict:
    """事件 -> 可持久化/传输的 dict。"""
    return {
        "v": SCHEMA_VERSION,
        "seq": event.seq,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "tenant_id": event.tenant_id,
        "request_id": event.request_id,
        "idempotency_key": event.idempotency_key,
        "timestamp": event.timestamp,
        "payload": event.payload,
        "prev_hash": event.prev_hash,
        "event_hash": event.event_hash,
    }


def decode_event(raw: dict) -> Event:
    """dict -> 事件，仅做结构解析，不做链校验。

    结构损坏（缺字段、类型错）归类为 EventCorrupted。
    """
    missing = [f for f in _REQUIRED_FIELDS if f not in raw]
    if missing:
        raise EventCorrupted(f"事件缺少字段: {','.join(missing)}")
    if raw["v"] != SCHEMA_VERSION:
        raise EventCorrupted(f"不支持的事件格式版本: {raw['v']!r}")
    try:
        seq = int(raw["seq"])
        payload = dict(raw["payload"])
    except (TypeError, ValueError) as exc:
        raise EventCorrupted(f"事件字段类型错误: {exc}") from exc
    return Event(
        seq=seq,
        event_id=str(raw["event_id"]),
        event_type=str(raw["event_type"]),
        tenant_id=str(raw["tenant_id"]),
        request_id=raw["request_id"],
        idempotency_key=raw["idempotency_key"],
        timestamp=str(raw["timestamp"]),
        payload=payload,
        prev_hash=str(raw["prev_hash"]),
        event_hash=str(raw["event_hash"]),
    )


def verify_event(event: Event, *, expected_seq: int, prev_hash: str) -> None:
    """校验单条事件：自身哈希、序号连续、哈希链连接。

    失败原因可区分：
      EventCorrupted     —— 载荷/哈希被篡改
      EventDuplicateError —— 序号回退（旧事件重复出现）
      EventOrderError    —— 序号跳跃（缺事件）或哈希链断裂
    """
    recomputed = compute_event_hash(
        prev_hash=event.prev_hash,
        seq=event.seq,
        event_id=event.event_id,
        event_type=event.event_type,
        tenant_id=event.tenant_id,
        request_id=event.request_id,
        idempotency_key=event.idempotency_key,
        timestamp=event.timestamp,
        payload=event.payload,
    )
    if recomputed != event.event_hash:
        raise EventCorrupted(
            f"事件 {event.seq}({event.event_id}) 校验和不匹配：日志可能已损坏"
        )
    if event.seq < expected_seq:
        raise EventDuplicateError(
            f"事件序号 {event.seq} 小于期望序号 {expected_seq}：检测到重复事件"
        )
    if event.seq > expected_seq:
        raise EventOrderError(
            f"事件序号 {event.seq} 跳跃，期望 {expected_seq}：检测到缺失/乱序事件"
        )
    if event.prev_hash != prev_hash:
        raise EventOrderError(
            f"事件 {event.seq} 的 prev_hash 与链尾不一致：哈希链断裂/乱序"
        )
