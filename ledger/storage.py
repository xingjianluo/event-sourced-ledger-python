"""本地持久化存储（文件模拟）。

目录结构：
  <base_dir>/<tenant_id>/events.log        追加式事件日志，每行一个 JSON
  <base_dir>/<tenant_id>/idem/<safe_key>   幂等记录，一条一文件
  <base_dir>/<tenant_id>/checkpoint.json   最新检查点（可选）
  <base_dir>/registry.json                 租户注册表

写入采用"临时文件 + fsync + os.replace"原子替换，
事件日志采用 O_APPEND 单行追加 + fsync；进程崩溃后要么读到旧状态，
要么读到一条完整记录，不会读到半条 JSON。
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading
from dataclasses import asdict
from pathlib import Path

from .errors import EventCorrupted
from .events import decode_event, encode_event
from .models import Checkpoint, Event, IdempotencyRecord


def _atomic_write_json(path: Path, obj: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, sort_keys=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
        # fsync 目录，保证 rename 元数据落盘
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


class TenantStore:
    """单个租户的事件日志与幂等记录存储。"""

    def __init__(self, base_dir: str, tenant_id: str) -> None:
        self.base_dir = Path(base_dir)
        self.tenant_id = tenant_id
        self.dir = self.base_dir / tenant_id
        self.events_path = self.dir / "events.log"
        self.idem_dir = self.dir / "idem"
        self.checkpoint_path = self.dir / "checkpoint.json"

    # ---- 事件日志 ----
    def append_event(self, event: Event) -> None:
        """把一条事件原子追加到日志末尾（单条 JSON 一行 + fsync）。"""
        self.dir.mkdir(parents=True, exist_ok=True)
        line = json.dumps(encode_event(event), ensure_ascii=False, sort_keys=True)
        # "a" 模式在 POSIX 上使用 O_APPEND，单次 write 对小于 PIPE_BUF/页大小
        # 的行是原子的；服务层另有租户锁串行化，双保险。
        with open(self.events_path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def read_events(self) -> list[Event]:
        """读出全部事件；损坏的行会以 EventCorrupted 明确报出（行号可定位）。"""
        if not self.events_path.exists():
            return []
        events: list[Event] = []
        with open(self.events_path, encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise EventCorrupted(
                        f"事件日志第 {lineno} 行不是合法 JSON：{exc.msg}"
                    ) from exc
                try:
                    events.append(decode_event(raw))
                except EventCorrupted as exc:
                    raise EventCorrupted(
                        f"事件日志第 {lineno} 行损坏：{exc.message}"
                    ) from exc
        return events

    def last_seq_and_hash(self) -> tuple[int | None, str]:
        """读取当前日志末尾的 seq 与哈希（空日志返回 (None, "")）。"""
        events = self.read_events()
        if not events:
            return None, ""
        return events[-1].seq, events[-1].event_hash

    # ---- 幂等记录 ----
    @staticmethod
    def _safe_key(key: str) -> str:
        # 只保留安全字符，其余做百分号转义，避免路径穿越。
        return re.sub(r"[^A-Za-z0-9_.-]", lambda m: f"%{ord(m.group()):02X}", key)

    def _idem_path(self, key: str) -> Path:
        return self.idem_dir / self._safe_key(key)

    def save_idempotency(self, record: IdempotencyRecord) -> None:
        _atomic_write_json(self._idem_path(record.idempotency_key), asdict(record))

    def load_idempotency(self, key: str) -> IdempotencyRecord | None:
        path = self._idem_path(key)
        if not path.exists():
            return None
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
        return IdempotencyRecord(**raw)

    # ---- 检查点 ----
    def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        _atomic_write_json(self.checkpoint_path, asdict(checkpoint))

    def load_checkpoint(self) -> Checkpoint | None:
        if not self.checkpoint_path.exists():
            return None
        with open(self.checkpoint_path, encoding="utf-8") as fh:
            raw = json.load(fh)
        return Checkpoint(**raw)


class Registry:
    """租户注册表：api_key 与租户的绑定关系。"""

    def __init__(self, base_dir: str) -> None:
        self.base_dir = Path(base_dir)
        self.path = self.base_dir / "registry.json"
        self._lock = threading.Lock()

    def _read(self) -> dict:
        if not self.path.exists():
            return {"tenants": {}}
        with open(self.path, encoding="utf-8") as fh:
            return json.load(fh)

    def _write(self, data: dict) -> None:
        _atomic_write_json(self.path, data)

    def register(self, tenant_id: str, name: str, api_key: str, now_iso: str) -> None:
        with self._lock:
            data = self._read()
            tenants = data.setdefault("tenants", {})
            tenants[tenant_id] = {
                "tenant_id": tenant_id,
                "name": name,
                "api_key": api_key,
                "created_at": now_iso,
            }
            self._write(data)

    def resolve_api_key(self, api_key: str) -> str | None:
        """api_key -> tenant_id，不存在返回 None。"""
        for t in self._read().get("tenants", {}).values():
            if t["api_key"] == api_key:
                return t["tenant_id"]
        return None

    def exists(self, tenant_id: str) -> bool:
        return tenant_id in self._read().get("tenants", {})

    def get_api_key(self, tenant_id: str) -> str | None:
        t = self._read().get("tenants", {}).get(tenant_id)
        return t["api_key"] if t else None


class Storage:
    """存储门面：聚合注册表与各租户存储。"""

    def __init__(self, base_dir: str) -> None:
        self.base_dir = base_dir
        Path(base_dir).mkdir(parents=True, exist_ok=True)

    def registry(self) -> Registry:
        return Registry(self.base_dir)

    def tenant(self, tenant_id: str) -> TenantStore:
        return TenantStore(self.base_dir, tenant_id)
