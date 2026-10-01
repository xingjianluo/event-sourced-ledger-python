"""pytest 公共夹具：临时存储目录、可控时钟、新鲜服务。"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from ledger.clock import Clock
from ledger.service import LedgerService
from ledger.storage import Storage


class SeqClock(Clock):
    """每调用一次推进 1 毫秒，保证同测内时间戳唯一且确定。"""

    def __init__(self, start: str = "2026-10-01T00:00:00.000000+00:00") -> None:
        self._n = 0
        self._start = start

    def now_iso(self) -> str:
        from datetime import UTC, datetime, timedelta

        base = datetime.fromisoformat(self._start)
        return (base + timedelta(microseconds=self._n)).astimezone(UTC).isoformat()

    def tick(self) -> None:
        self._n += 1


@pytest.fixture()
def tmp_home(tmp_path: Path) -> Path:
    home = tmp_path / "ledger-data"
    return home


@pytest.fixture()
def service(tmp_home: Path) -> LedgerService:
    clock = SeqClock()
    svc = LedgerService(Storage(str(tmp_home)), clock)
    yield svc
    clock.tick()


@pytest.fixture()
def two_account_tenant(service: LedgerService) -> dict:
    """建一个含两个 CNY 账户的租户：a=1000, b=0。"""
    service.create_tenant("t1", "租户甲", "key-tenant-001", currencies=("CNY",))
    service.open_account("t1", "a", "CNY", initial_balance=1000)
    service.open_account("t1", "b", "CNY", initial_balance=0)
    return {"tenant": "t1", "api_key": "key-tenant-001", "a": "a", "b": "b"}


@pytest.fixture(autouse=True)
def _caplog_info(caplog: pytest.LogCaptureFixture):
    """让每个测试默认能捕获 INFO 级账本日志（含请求标识与流水）。"""
    caplog.set_level(logging.INFO, logger="ledger")
