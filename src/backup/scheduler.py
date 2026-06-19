"""Backup scheduler — evaluates cron expressions and fires due events."""

import asyncio
import time
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field

from src.metadata.store import MetadataStore, BackupType, BackupPolicy


@dataclass
class ScheduleEntry:
    policy_id: str
    cron_parts: List[int]  # [minute, hour, day_of_month, month, day_of_week]
    backup_type: BackupType
    next_run: float
    interval_seconds: int  # Simplified interval for simulation


def parse_cron_simple(expr: str) -> List[int]:
    """Parse simple cron expressions like '0 */6 * * *' or '0 2 * * 0'."""
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {expr}")
    result = []
    for part in parts:
        if part == '*':
            result.append(-1)
        elif part.startswith('*/'):
            result.append(int(part[2:]))
        else:
            result.append(int(part))
    return result


class BackupScheduler:
    """Simulated backup scheduler that fires due events."""

    def __init__(self, metadata_store: MetadataStore):
        self.metadata = metadata_store
        self._entries: Dict[str, ScheduleEntry] = {}
        self._listeners: List[Callable] = []
        self._running = False

    def add_policy(self, policy: BackupPolicy) -> ScheduleEntry:
        cron_parts = parse_cron_simple(policy.schedule_cron)
        # Compute next run: for simulation, use interval if '*/N' present
        interval = 3600  # default 1h
        if cron_parts[1] > 0:
            interval = cron_parts[1] * 3600
        elif cron_parts[0] > 0 and cron_parts[0] < 60:
            interval = cron_parts[0] * 60
        entry = ScheduleEntry(
            policy_id=policy.id,
            cron_parts=cron_parts,
            backup_type=policy.backup_type,
            next_run=time.time() + interval,
            interval_seconds=interval,
        )
        self._entries[policy.id] = entry
        return entry

    def on_backup_due(self, listener: Callable) -> None:
        self._listeners.append(listener)

    def check_due(self) -> List[str]:
        """Return policy_ids whose schedules are due."""
        now = time.time()
        due = []
        for entry in self._entries.values():
            if now >= entry.next_run:
                due.append(entry.policy_id)
                entry.next_run = now + entry.interval_seconds
        return due

    async def run_loop(self, interval_sec: float = 1.0) -> None:
        self._running = True
        while self._running:
            due = self.check_due()
            for policy_id in due:
                for listener in self._listeners:
                    await listener(policy_id)
            await asyncio.sleep(interval_sec)

    def stop(self) -> None:
        self._running = False
