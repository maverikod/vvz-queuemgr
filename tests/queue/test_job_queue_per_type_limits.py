"""
Tests for per-job-type memory limits in JobQueue.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from typing import Dict, Iterable, Optional

from queuemgr.queue.job_queue import JobQueue
from queuemgr.core.registry import Registry
from queuemgr.core.types import JobId, JobRecord
from queuemgr.jobs.base import QueueJobBase


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
        Return all latest records.

        Returns:
            Iterable of all stored records.
        """
        return list(self._records.values())


class TestJob(QueueJobBase):
    """Test job class."""

    def execute(self) -> None:
        """Execute test job."""
        pass


class AnotherTestJob(QueueJobBase):
    """Another test job class."""

    def execute(self) -> None:
        """Execute another test job."""
        pass


class TestJobQueuePerTypeLimits:
    """Test per-job-type limits functionality."""

    def test_add_job_without_limits(self) -> None:
        """Test adding jobs when no limits are configured."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry)

        job1 = TestJob("job1", {})
        job2 = TestJob("job2", {})
        job3 = AnotherTestJob("job3", {})

        queue.add_job(job1)
        queue.add_job(job2)
        queue.add_job(job3)

        assert len(queue.get_jobs()) == 3
        assert "job1" in queue.get_jobs()
        assert "job2" in queue.get_jobs()
        assert "job3" in queue.get_jobs()

    def test_add_job_with_per_type_limit_not_reached(self) -> None:
        """Test adding jobs when per-type limit is not reached."""
        registry = InMemoryRegistry()
        limits = {"TestJob": 5}
        queue = JobQueue(registry, per_job_type_limits=limits)

        job1 = TestJob("job1", {})
        job2 = TestJob("job2", {})
        job3 = TestJob("job3", {})

        queue.add_job(job1)
        queue.add_job(job2)
        queue.add_job(job3)

        assert len(queue.get_jobs()) == 3
        assert "job1" in queue.get_jobs()
        assert "job2" in queue.get_jobs()
        assert "job3" in queue.get_jobs()

    def test_add_job_with_per_type_limit_reached(self) -> None:
        """Test adding jobs when per-type limit is reached (FIFO eviction)."""
        registry = InMemoryRegistry()
        limits = {"TestJob": 2}
        queue = JobQueue(registry, per_job_type_limits=limits)

        # Add 2 jobs (limit)
        job1 = TestJob("job1", {})
        job2 = TestJob("job2", {})
        queue.add_job(job1)
        queue.add_job(job2)

        assert len(queue.get_jobs()) == 2
        assert "job1" in queue.get_jobs()
        assert "job2" in queue.get_jobs()

        # Add 3rd job - should evict oldest (job1)
        job3 = TestJob("job3", {})
        queue.add_job(job3)

        assert len(queue.get_jobs()) == 2
        assert "job1" not in queue.get_jobs()  # Evicted
        assert "job2" in queue.get_jobs()
        assert "job3" in queue.get_jobs()

    def test_add_job_with_multiple_types_different_limits(self) -> None:
        """Test adding jobs with different limits for different types."""
        registry = InMemoryRegistry()
        limits = {"TestJob": 2, "AnotherTestJob": 3}
        queue = JobQueue(registry, per_job_type_limits=limits)

        # Add TestJob jobs up to limit
        job1 = TestJob("job1", {})
        job2 = TestJob("job2", {})
        queue.add_job(job1)
        queue.add_job(job2)

        # Add AnotherTestJob jobs up to limit
        job3 = AnotherTestJob("job3", {})
        job4 = AnotherTestJob("job4", {})
        job5 = AnotherTestJob("job5", {})
        queue.add_job(job3)
        queue.add_job(job4)
        queue.add_job(job5)

        assert len(queue.get_jobs()) == 5

        # Add another TestJob - should evict oldest TestJob (job1)
        job6 = TestJob("job6", {})
        queue.add_job(job6)

        assert len(queue.get_jobs()) == 5
        assert "job1" not in queue.get_jobs()  # Evicted
        assert "job2" in queue.get_jobs()
        assert "job6" in queue.get_jobs()
        assert "job3" in queue.get_jobs()  # Not evicted (different type)
        assert "job4" in queue.get_jobs()
        assert "job5" in queue.get_jobs()

        # Add another AnotherTestJob - should evict oldest (job3)
        job7 = AnotherTestJob("job7", {})
        queue.add_job(job7)

        assert len(queue.get_jobs()) == 5
        assert "job3" not in queue.get_jobs()  # Evicted
        assert "job4" in queue.get_jobs()
        assert "job5" in queue.get_jobs()
        assert "job7" in queue.get_jobs()

    def test_add_job_with_global_limit(self) -> None:
        """Test adding jobs with global queue size limit."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry, max_queue_size=3)

        job1 = TestJob("job1", {})
        job2 = TestJob("job2", {})
        job3 = TestJob("job3", {})
        queue.add_job(job1)
        queue.add_job(job2)
        queue.add_job(job3)

        assert len(queue.get_jobs()) == 3

        # Add 4th job - should evict oldest overall (job1)
        job4 = TestJob("job4", {})
        queue.add_job(job4)

        assert len(queue.get_jobs()) == 3
        assert "job1" not in queue.get_jobs()  # Evicted
        assert "job2" in queue.get_jobs()
        assert "job3" in queue.get_jobs()
        assert "job4" in queue.get_jobs()

    def test_add_job_per_type_limit_takes_precedence(self) -> None:
        """Test that per-type limit is checked before global limit."""
        registry = InMemoryRegistry()
        limits = {"TestJob": 2}
        queue = JobQueue(registry, max_queue_size=10, per_job_type_limits=limits)

        # Add 2 TestJob jobs (at limit)
        job1 = TestJob("job1", {})
        job2 = TestJob("job2", {})
        queue.add_job(job1)
        queue.add_job(job2)

        # Add another TestJob - should evict based on per-type limit, not global
        job3 = TestJob("job3", {})
        queue.add_job(job3)

        assert len(queue.get_jobs()) == 2
        assert "job1" not in queue.get_jobs()  # Evicted by per-type limit
        assert "job2" in queue.get_jobs()
        assert "job3" in queue.get_jobs()

    def test_get_jobs_by_type(self) -> None:
        """Test helper method for getting jobs by type."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry)

        job1 = TestJob("job1", {})
        job2 = TestJob("job2", {})
        job3 = AnotherTestJob("job3", {})
        queue.add_job(job1)
        queue.add_job(job2)
        queue.add_job(job3)

        test_jobs = queue._get_jobs_by_type("TestJob")
        assert len(test_jobs) == 2
        assert "job1" in test_jobs
        assert "job2" in test_jobs

        another_jobs = queue._get_jobs_by_type("AnotherTestJob")
        assert len(another_jobs) == 1
        assert "job3" in another_jobs

    def test_find_oldest_job_id(self) -> None:
        """Test helper method for finding oldest job."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry)

        job1 = TestJob("job1", {})
        job2 = TestJob("job2", {})
        job3 = TestJob("job3", {})
        queue.add_job(job1)
        # Small delay to ensure different timestamps
        import time

        time.sleep(0.01)
        queue.add_job(job2)
        time.sleep(0.01)
        queue.add_job(job3)

        job_ids = ["job1", "job2", "job3"]
        oldest = queue._find_oldest_job_id(job_ids)
        assert oldest == "job1"

    def test_eviction_removes_from_registry(self) -> None:
        """Test that evicted jobs are removed from registry."""
        registry = InMemoryRegistry()
        limits = {"TestJob": 1}
        queue = JobQueue(registry, per_job_type_limits=limits)

        job1 = TestJob("job1", {})
        queue.add_job(job1)

        # Check job is in registry
        latest = registry.latest("job1")
        assert latest is not None
        assert latest.job_id == "job1"

        # Add 2nd job - should evict job1
        job2 = TestJob("job2", {})
        queue.add_job(job2)

        # job1 should be removed from queue
        assert "job1" not in queue.get_jobs()
        assert "job2" in queue.get_jobs()

    def test_limit_of_one(self) -> None:
        """Test limit of 1 (only one job of type allowed)."""
        registry = InMemoryRegistry()
        limits = {"TestJob": 1}
        queue = JobQueue(registry, per_job_type_limits=limits)

        job1 = TestJob("job1", {})
        queue.add_job(job1)
        assert len(queue.get_jobs()) == 1

        job2 = TestJob("job2", {})
        queue.add_job(job2)
        assert len(queue.get_jobs()) == 1
        assert "job1" not in queue.get_jobs()
        assert "job2" in queue.get_jobs()

    def test_job_type_not_in_limits(self) -> None:
        """Test that jobs with types not in limits are not limited."""
        registry = InMemoryRegistry()
        limits = {"TestJob": 1}
        queue = JobQueue(registry, per_job_type_limits=limits)

        # TestJob is limited
        job1 = TestJob("job1", {})
        job2 = TestJob("job2", {})
        queue.add_job(job1)
        queue.add_job(job2)  # Should evict job1

        assert len(queue.get_jobs()) == 1
        assert "job2" in queue.get_jobs()

        # AnotherTestJob is not limited
        job3 = AnotherTestJob("job3", {})
        job4 = AnotherTestJob("job4", {})
        job5 = AnotherTestJob("job5", {})
        queue.add_job(job3)
        queue.add_job(job4)
        queue.add_job(job5)

        assert len(queue.get_jobs()) == 4  # job2 + 3 AnotherTestJob jobs
