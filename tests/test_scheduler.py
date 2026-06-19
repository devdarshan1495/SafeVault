"""Tests for the backup scheduler."""

import time
from src.backup.scheduler import parse_cron_simple, BackupScheduler, ScheduleEntry
from src.metadata.store import MetadataStore, BackupType


class TestParseCron:
    def test_every_6_hours(self):
        parts = parse_cron_simple("0 */6 * * *")
        assert parts == [0, 6, -1, -1, -1]

    def test_daily_at_2am(self):
        parts = parse_cron_simple("0 2 * * *")
        assert parts == [0, 2, -1, -1, -1]

    def test_weekly_sunday(self):
        parts = parse_cron_simple("0 2 * * 0")
        assert parts == [0, 2, -1, -1, 0]

    def test_every_minute(self):
        parts = parse_cron_simple("* * * * *")
        assert parts == [-1, -1, -1, -1, -1]

    def test_invalid_expression(self):
        import pytest
        with pytest.raises(ValueError):
            parse_cron_simple("invalid")


class TestBackupScheduler:
    def setup_method(self):
        self.store = MetadataStore()
        self.scheduler = BackupScheduler(self.store)

    def test_add_policy(self):
        policy = self.store.create_policy(
            "u1", "hourly", "0 * * * *", 7, BackupType.FULL, ["/data"],
        )
        entry = self.scheduler.add_policy(policy)
        assert entry.policy_id == policy.id
        assert entry.backup_type == BackupType.FULL
        assert entry.interval_seconds == 3600

    def test_check_due_initially_none(self):
        policy = self.store.create_policy(
            "u1", "test", "0 2 * * *", 7, BackupType.FULL, ["/data"],
        )
        self.scheduler.add_policy(policy)
        due = self.scheduler.check_due()
        assert len(due) == 0

    def test_due_triggers(self):
        policy = self.store.create_policy(
            "u1", "frequent", "*/1 * * * *", 7, BackupType.INCREMENTAL, ["/data"],
        )
        self.scheduler.add_policy(policy)
        # Next run is in the past if interval is small
        # Force next_run to past to trigger
        for entry in self.scheduler._entries.values():
            entry.next_run = time.time() - 1
        due = self.scheduler.check_due()
        assert len(due) == 1
        assert due[0] == policy.id

    def test_multiple_policies(self):
        p1 = self.store.create_policy(
            "u1", "a", "0 * * * *", 7, BackupType.FULL, ["/a"],
        )
        p2 = self.store.create_policy(
            "u1", "b", "0 * * * *", 7, BackupType.FULL, ["/b"],
        )
        self.scheduler.add_policy(p1)
        self.scheduler.add_policy(p2)
        for entry in self.scheduler._entries.values():
            entry.next_run = time.time() - 1
        due = self.scheduler.check_due()
        assert len(due) == 2
