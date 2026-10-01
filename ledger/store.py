"""本地事件存储：JSONL 追加日志 + 检查点文件。

- 每条事件一行 JSON，带 payload 校验和；
- 读取时严格校验：seq 必须连续递增（乱序）、不得重复（重复）、
  校验和必须匹配（损坏），分别以不同错误拒绝；
- 写入使用追加 + fsync，崩溃后已落盘事件不丢。
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from .errors import CorruptEventError, DuplicateEventError, OutOfOrderEventError
from .models import Checkpoint, Event


def compute_checksum(tenant_id: str, seq: int, event_type: str, payload: dict) -> str:
    """对事件关键字段计算稳定校验和。"""
    canonical = json.dumps(
        {
            "tenant_id": tenant_id,
            "seq": seq,
            "event_type": event_type,
            "payload": payload,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class EventStore:
    """追加式事件日志，带校验和，可检测乱序/重复/损坏。"""

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._log_path = self._data_dir / "events.jsonl"
        self._checkpoint_path = self._data_dir / "checkpoint.json"
        self._log_path.touch(exist_ok=True)

    def append(self, event: Event) -> None:
        """追加一条事件并落盘。调用方负责保证 seq 正确。"""
        record = {
            "seq": event.seq,
            "tenant_id": event.tenant_id,
            "event_type": event.event_type,
            "payload": event.payload,
            "checksum": event.checksum,
        }
        line = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
        with open(self._log_path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def load_events(self) -> list[Event]:
        """读取并校验全部事件；乱序/重复/损坏分别以不同错误拒绝。"""
        events: list[Event] = []
        seen_seqs: set[int] = set()
        expected_seq = 1
        with open(self._log_path, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    event = Event(
                        seq=int(record["seq"]),
                        tenant_id=str(record["tenant_id"]),
                        event_type=str(record["event_type"]),
                        payload=dict(record["payload"]),
                        checksum=str(record["checksum"]),
                    )
                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                    raise CorruptEventError(
                        f"line {line_no}: unparsable event record"
                    ) from exc

                expected_checksum = compute_checksum(
                    event.tenant_id, event.seq, event.event_type, event.payload
                )
                if event.checksum != expected_checksum:
                    raise CorruptEventError(
                        f"line {line_no}: checksum mismatch for seq {event.seq}"
                    )
                if event.seq in seen_seqs:
                    raise DuplicateEventError(
                        f"line {line_no}: duplicate seq {event.seq}"
                    )
                if event.seq != expected_seq:
                    raise OutOfOrderEventError(
                        f"line {line_no}: expected seq {expected_seq}, got {event.seq}"
                    )
                seen_seqs.add(event.seq)
                expected_seq += 1
                events.append(event)
        return events

    def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        record = {"seq": checkpoint.seq, "balances": checkpoint.balances}
        tmp = self._checkpoint_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(record, f, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._checkpoint_path)

    def load_checkpoint(self) -> Checkpoint:
        if not self._checkpoint_path.exists():
            return Checkpoint(seq=0, balances={})
        with open(self._checkpoint_path, "r", encoding="utf-8") as f:
            record = json.load(f)
        return Checkpoint(
            seq=int(record["seq"]),
            balances={
                str(t): {str(a): int(v) for a, v in accts.items()}
                for t, accts in record["balances"].items()
            },
        )
