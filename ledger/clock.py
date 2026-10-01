"""时间工具（可替换为可控时钟，便于测试回放确定性）。

所有时间戳均为 UTC，形如 2026-10-01T08:00:00.123456+00:00。
"""

from __future__ import annotations

from datetime import UTC, datetime


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class Clock:
    """时钟接口，测试中可注入固定时钟。"""

    def now_iso(self) -> str:
        raise NotImplementedError


class SystemClock(Clock):
    def now_iso(self) -> str:
        return utc_now_iso()


class FixedClock(Clock):
    """每次调用返回预设时间的时钟；可通过 advance 推进。"""

    def __init__(self, value: str | None = None) -> None:
        self.value = value or "2026-10-01T00:00:00+00:00"

    def now_iso(self) -> str:
        return self.value

    def advance(self, value: str) -> None:
        self.value = value
