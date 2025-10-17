"""
Tests for JobQueue basic functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import pytest
from unittest.mock import Mock, patch

from queuemgr.queue.job_queue import JobQueue
from queuemgr.jobs.base import QueueJobBase
from queuemgr.exceptions import JobNotFoundError, JobAlreadyExistsError


class TestJob(QueueJobBase):
    """Test job implementation."""
    
    def __init__(self, job_id: str, params: dict):
        """
        Initialize TestJob.
        
        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)
    
    def execute(self) -> None:
        """Execute the job."""
        pass


class TestJobQueueBasic:
    """Test JobQueue basic functionality."""

    def test_queue_initialization(self):
        """Test queue initialization."""
        queue = JobQueue()
        
        assert queue._jobs == {}
        assert queue._job_creation_times == {}
        assert queue._manager is not None
        assert queue._registry is not None

    def test_get_jobs_empty(self):
        """Test getting jobs from empty queue."""
        queue = JobQueue()
        
        jobs = queue.get_jobs()
        assert jobs == []

    def test_get_jobs_with_jobs(self):
        """Test getting jobs from queue with jobs."""
        queue = JobQueue()
        
        # Add test job
        job = TestJob("test-job-1", {})
        queue._jobs["test-job-1"] = job
        
        jobs = queue.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].job_id == "test-job-1"

    def test_get_job_status_not_found(self):
        """Test getting status of non-existent job."""
        queue = JobQueue()
        
        with pytest.raises(JobNotFoundError):
            queue.get_job_status("non-existent")

    def test_get_job_status_found(self):
        """Test getting status of existing job."""
        queue = JobQueue()
        
        # Add test job
        job = TestJob("test-job-1", {})
        queue._jobs["test-job-1"] = job
        
        status = queue.get_job_status("test-job-1")
        assert status["job_id"] == "test-job-1"
        assert "status" in status
        assert "created_at" in status

    def test_add_job_success(self):
        """Test successful job addition."""
        queue = JobQueue()
        
        queue.add_job(TestJob, "test-job-1", {"param1": "value1"})
        
        assert "test-job-1" in queue._jobs
        assert "test-job-1" in queue._job_creation_times
        assert queue._jobs["test-job-1"].job_id == "test-job-1"
        assert queue._jobs["test-job-1"].params == {"param1": "value1"}

    def test_add_job_already_exists(self):
        """Test adding job that already exists."""
        queue = JobQueue()
        
        # Add job first time
        queue.add_job(TestJob, "test-job-1", {})
        
        # Try to add same job again
        with pytest.raises(JobAlreadyExistsError):
            queue.add_job(TestJob, "test-job-1", {})

    def test_delete_job_not_found(self):
        """Test deleting non-existent job."""
        queue = JobQueue()
        
        with pytest.raises(JobNotFoundError):
            queue.delete_job("non-existent")

    def test_delete_job_not_running(self):
        """Test deleting job that is not running."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Delete job
        queue.delete_job("test-job-1")
        
        assert "test-job-1" not in queue._jobs
        assert "test-job-1" not in queue._job_creation_times

    def test_get_job_count(self):
        """Test getting job count."""
        queue = JobQueue()
        
        assert queue.get_job_count() == 0
        
        # Add jobs
        queue.add_job(TestJob, "test-job-1", {})
        queue.add_job(TestJob, "test-job-2", {})
        
        assert queue.get_job_count() == 2

    def test_get_running_jobs(self):
        """Test getting running jobs."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Mock job as running
        job = queue._jobs["test-job-1"]
        job._process = Mock()
        job._process.is_alive.return_value = True
        
        running_jobs = queue.get_running_jobs()
        assert len(running_jobs) == 1
        assert "test-job-1" in running_jobs

    def test_get_job_by_id(self):
        """Test getting job by ID."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        job = queue.get_job_by_id("test-job-1")
        assert job is not None
        assert job.job_id == "test-job-1"
        
        # Test non-existent job
        job = queue.get_job_by_id("non-existent")
        assert job is None

    def test_list_job_statuses(self):
        """Test listing job statuses."""
        queue = JobQueue()
        
        # Add test jobs
        queue.add_job(TestJob, "test-job-1", {})
        queue.add_job(TestJob, "test-job-2", {})
        
        statuses = queue.list_job_statuses()
        assert len(statuses) == 2
        assert "test-job-1" in statuses
        assert "test-job-2" in statuses
