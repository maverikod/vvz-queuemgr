"""
Tests for JobQueue registry loading functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from queuemgr.queue.job_queue import JobQueue
from queuemgr.core.registry import Registry
from queuemgr.core.types import JobId, JobRecord, JobStatus
from queuemgr.jobs.base import QueueJobBase
from queuemgr.jobs.registry_job import RegistryPlaceholderJob


class InMemoryRegistry(Registry):
    """
    In-memory Registry implementation for tests.
    """

    def __init__(self, records: Optional[List[JobRecord]] = None) -> None:
        """Initialize registry with optional initial records."""
        self._records: Dict[JobId, JobRecord] = {}
        if records:
            for record in records:
                self._records[record.job_id] = record

    def append(self, record: JobRecord) -> None:
        """Store the latest record for a job."""
        self._records[record.job_id] = record

    def latest(self, job_id: JobId) -> Optional[JobRecord]:
        """Return the latest record for a job, if stored."""
        return self._records.get(job_id)

    def all_latest(self) -> Iterable[JobRecord]:
        """Return all latest records."""
        return list(self._records.values())


class TestJob(QueueJobBase):
    """Test job class."""

    def execute(self) -> None:
        """Execute test job."""
        pass


class TestJobQueueRegistryLoading:
    """Test registry loading functionality."""

    def test_load_jobs_from_empty_registry(self) -> None:
        """Test loading from empty registry."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry)

        jobs = queue.list_jobs()
        assert len(jobs) == 0

    def test_load_jobs_from_registry(self) -> None:
        """Test loading jobs from registry."""
        now = datetime.now()
        records = [
            JobRecord(
                job_id="job1",
                status=JobStatus.PENDING,
                progress=0,
                description="Job 1",
                result=None,
                created_at=now,
                updated_at=now,
            ),
            JobRecord(
                job_id="job2",
                status=JobStatus.COMPLETED,
                progress=100,
                description="Job 2 completed",
                result={"output": "success"},
                created_at=now,
                updated_at=now,
            ),
        ]

        registry = InMemoryRegistry(records)
        queue = JobQueue(registry)

        jobs = queue.list_jobs()
        assert len(jobs) == 2

        # Check job1
        job1_status = queue.get_job_status("job1")
        assert job1_status.job_id == "job1"
        assert job1_status.status == JobStatus.PENDING
        assert job1_status.progress == 0

        # Check job2
        job2_status = queue.get_job_status("job2")
        assert job2_status.job_id == "job2"
        assert job2_status.status == JobStatus.COMPLETED
        assert job2_status.progress == 100
        assert job2_status.result == {"output": "success"}

    def test_load_jobs_from_file_registry(self) -> None:
        """Test loading jobs from actual JSONL file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            registry_path = f.name
            records = [
                {
                    "job_id": "file_job_1",
                    "status": 0,
                    "progress": 25,
                    "description": "File job 1",
                    "result": None,
                    "created_at": "2025-01-27T00:00:00",
                    "updated_at": "2025-01-27T01:00:00",
                },
                {
                    "job_id": "file_job_2",
                    "status": 2,
                    "progress": 100,
                    "description": "File job 2 completed",
                    "result": {"data": "test"},
                    "created_at": "2025-01-27T00:00:00",
                    "updated_at": "2025-01-27T02:00:00",
                },
            ]

            for record in records:
                f.write(json.dumps(record) + "\n")

        try:
            from queuemgr.core.registry import JsonlRegistry

            registry = JsonlRegistry(registry_path)
            queue = JobQueue(registry)

            jobs = queue.list_jobs()
            assert len(jobs) == 2

            # Check file_job_1
            status1 = queue.get_job_status("file_job_1")
            assert status1.job_id == "file_job_1"
            assert status1.status == JobStatus.PENDING
            assert status1.progress == 25

            # Check file_job_2
            status2 = queue.get_job_status("file_job_2")
            assert status2.job_id == "file_job_2"
            assert status2.status == JobStatus.COMPLETED
            assert status2.progress == 100
            assert status2.result == {"data": "test"}

        finally:
            Path(registry_path).unlink(missing_ok=True)

    def test_loaded_jobs_are_placeholder(self) -> None:
        """Test that loaded jobs are placeholder jobs."""
        now = datetime.now()
        record = JobRecord(
            job_id="placeholder_job",
            status=JobStatus.PENDING,
            progress=0,
            description="Placeholder",
            result=None,
            created_at=now,
            updated_at=now,
        )

        registry = InMemoryRegistry([record])
        queue = JobQueue(registry)

        # Get the job object
        jobs = queue.get_jobs()
        assert "placeholder_job" in jobs

        job = jobs["placeholder_job"]
        assert isinstance(job, RegistryPlaceholderJob)
        assert job.is_running() is False

        # Try to execute - should raise error
        try:
            job.execute()
            assert False, "Should have raised RuntimeError"
        except RuntimeError as e:
            assert "cannot be executed" in str(e)

    def test_add_job_after_loading_from_registry(self) -> None:
        """Test adding new job after loading from registry."""
        now = datetime.now()
        record = JobRecord(
            job_id="existing_job",
            status=JobStatus.PENDING,
            progress=0,
            description="Existing",
            result=None,
            created_at=now,
            updated_at=now,
        )

        registry = InMemoryRegistry([record])
        queue = JobQueue(registry)

        # Verify existing job is loaded
        assert len(queue.list_jobs()) == 1

        # Add new job
        new_job = TestJob("new_job", {})
        queue.add_job(new_job)

        # Should have both jobs
        jobs = queue.list_jobs()
        assert len(jobs) == 2
        assert any(j["job_id"] == "existing_job" for j in jobs)
        assert any(j["job_id"] == "new_job" for j in jobs)

    def test_get_job_status_for_loaded_job(self) -> None:
        """Test getting status for job loaded from registry."""
        now = datetime.now()
        record = JobRecord(
            job_id="status_job",
            status=JobStatus.ERROR,
            progress=50,
            description="Error occurred",
            result={"error": "Test error"},
            created_at=now,
            updated_at=now,
        )

        registry = InMemoryRegistry([record])
        queue = JobQueue(registry)

        status = queue.get_job_status("status_job")
        assert status.job_id == "status_job"
        assert status.status == JobStatus.ERROR
        assert status.progress == 50
        assert status.description == "Error occurred"
        assert status.result == {"error": "Test error"}
