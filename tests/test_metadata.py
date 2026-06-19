"""Tests for the metadata store."""

from src.metadata.store import (
    MetadataStore, BackupType, JobStatus, VersionStatus,
)


class TestMetadataStore:
    def setup_method(self):
        self.store = MetadataStore()

    def test_create_policy(self):
        policy = self.store.create_policy(
            user_id="u1",
            name="Daily",
            schedule_cron="0 2 * * *",
            retention_days=30,
            backup_type=BackupType.FULL,
            source_paths=["/data"],
        )
        assert policy.id is not None
        assert self.store.get_policy(policy.id) is policy

    def test_register_file(self):
        fm = self.store.register_file(
            "u1", "/data/file.txt", 1000, 1234567890, "hash123",
        )
        assert fm.id is not None
        assert self.store.get_file_by_path("/data/file.txt") is fm

    def test_duplicate_file_returns_same(self):
        fm1 = self.store.register_file("u1", "/data/x.txt", 100, 0, "h1")
        fm2 = self.store.register_file("u1", "/data/x.txt", 100, 0, "h1")
        assert fm1.id == fm2.id

    def test_version_chain(self):
        policy = self.store.create_policy(
            "u1", "test", "0 * * * *", 7, BackupType.FULL, ["/data"],
        )
        v1 = self.store.create_version(
            "job1", policy.id, "u1", 1, None, BackupType.FULL, 7,
        )
        v2 = self.store.create_version(
            "job2", policy.id, "u1", 2, v1.id, BackupType.INCREMENTAL, 7,
        )
        latest = self.store.get_latest_version(policy.id)
        assert latest.id == v2.id

    def test_chunk_registration(self):
        cr = self.store.register_chunk("hash123", 1000, 500)
        assert cr.hash == "hash123"
        assert cr.reference_count == 1

        # Duplicate increments ref count
        cr2 = self.store.register_chunk("hash123", 1000, 500)
        assert cr2.id == cr.id  # Same record
        assert cr2.reference_count == 2  # Incremented

    def test_job_lifecycle(self):
        policy = self.store.create_policy(
            "u1", "test", "0 * * * *", 7, BackupType.FULL, ["/data"],
        )
        job = self.store.create_job(policy.id, "u1", BackupType.FULL)
        assert job.status == JobStatus.PENDING

        self.store.update_job_status(job.id, JobStatus.RUNNING)
        assert self.store.jobs[job.id].status == JobStatus.RUNNING

        self.store.update_job_status(job.id, JobStatus.COMPLETED)
        assert self.store.jobs[job.id].status == JobStatus.COMPLETED

    def test_restore_job(self):
        rj = self.store.create_restore_job("u1", "version1", "/restore")
        assert rj.status == JobStatus.PENDING
        self.store.update_restore_job(rj.id, status=JobStatus.COMPLETED,
                                       files_restored=5)
        assert self.store.restore_jobs[rj.id].files_restored == 5
