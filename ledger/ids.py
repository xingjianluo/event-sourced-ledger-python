"""标识生成工具。"""

from __future__ import annotations

import uuid


def new_uuid() -> str:
    """不带连字符的随机 UUID，保证全局唯一且可作为文件名片段。"""
    return uuid.uuid4().hex


def new_transfer_id() -> str:
    return f"txn_{new_uuid()}"


def new_event_id() -> str:
    return f"evt_{new_uuid()}"


def new_request_id() -> str:
    return f"req_{new_uuid()}"
