"""
Tests for JobQueue.list_jobs serialization behavior.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import json
from datetime import datetime
from typing import Dict, Iterable, Optional

from queuemgr.core.registry import Registry
from queuemgr.core.types import JobId, JobRecord, JobStatus
from queuemgr.jobs.base import QueueJobBase
from queuemgr.queue.job_queue import JobQueue


class InMemoryRegistry(Registry):
    """
    In-memory Registry implementation for tests.

    Avoids filesystem I/O while satisfying Registry interface contracts.
    """

    def __init__(self) -> None:
        """Initialize empty registry storage."""
        self._records: Dict[JobId, JobRecord] = {}

    def append(self, record: JobRecord) -> None:
        """
        Store the latest record for a job.

        Args:
            record: Snapshot to persist.
        """
        self._records[record.job_id] = record

    def latest(self, job_id: JobId) -> Optional[JobRecord]:
        """
        Return the latest record for a job, if stored.

        Args:
            job_id: Identifier to look up.

        Returns:
            Latest record or None when missing.
        """
        return self._records.get(job_id)

    def all_latest(self) -> Iterable[JobRecord]:
        """
        Return latest records for all tracked jobs.

        Returns:
            Iterable containing the latest JobRecord for each job.
        """
        return list(self._records.values())


class SampleJob(QueueJobBase):
    """Minimal job used to populate the queue during tests."""

    def execute(self) -> None:
        """Execute job logic (no-op for tests)."""
        return None


class TestJobQueueListJobs:
    """Validate that JobQueue.list_jobs returns JSON-safe snapshots."""

    def test_list_jobs_returns_serializable_snapshots(self) -> None:
        """Ensure list_jobs output can be serialized and contains metadata."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry)
        queue.add_job(SampleJob("test-job", {"payload": "value"}))

        jobs = queue.list_jobs()

        assert len(jobs) == 1
        job_snapshot = jobs[0]

        assert job_snapshot["job_id"] == "test-job"
        assert job_snapshot["status"] == JobStatus.PENDING.name
        assert job_snapshot["progress"] == 0
        assert job_snapshot["is_running"] is False

        # Validate timestamp formatting
        datetime.fromisoformat(job_snapshot["created_at"])
        datetime.fromisoformat(job_snapshot["updated_at"])

        # Ensure JSON serialization succeeds without custom encoders
        json.dumps(jobs)
