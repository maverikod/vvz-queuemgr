"""
Tests for JobQueue job operations.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import pytest
from unittest.mock import Mock, patch

from queuemgr.queue.job_queue import JobQueue
from queuemgr.jobs.base import QueueJobBase
from queuemgr.exceptions import JobNotFoundError, InvalidJobStateError, ProcessControlError
from queuemgr.core.types import JobStatus


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


class TestJobQueueOperations:
    """Test JobQueue job operations."""

    def test_start_job_not_found(self):
        """Test starting non-existent job."""
        queue = JobQueue()
        
        with pytest.raises(JobNotFoundError):
            queue.start_job("non-existent")

    def test_start_job_invalid_state(self):
        """Test starting job in invalid state."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Mock job as already running
        job = queue._jobs["test-job-1"]
        job._process = Mock()
        job._process.is_alive.return_value = True
        
        with pytest.raises(InvalidJobStateError):
            queue.start_job("test-job-1")

    def test_start_job_already_running(self):
        """Test starting job that is already running."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Mock job as running
        job = queue._jobs["test-job-1"]
        job._process = Mock()
        job._process.is_alive.return_value = True
        
        with pytest.raises(InvalidJobStateError):
            queue.start_job("test-job-1")

    def test_start_job_success(self):
        """Test successful job start."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Mock job start
        with patch.object(queue._jobs["test-job-1"], 'start_process') as mock_start:
            with patch('queuemgr.queue.job_queue.set_command') as mock_set_command:
                # Mock shared state
                queue._jobs["test-job-1"]._shared_state = {"command": Mock()}
                
                queue.start_job("test-job-1")
                
                mock_start.assert_called_once()
                mock_set_command.assert_called_once()

    def test_start_job_process_failure(self):
        """Test job start with process failure."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Mock job start to raise exception
        with patch.object(queue._jobs["test-job-1"], 'start_process') as mock_start:
            mock_start.side_effect = ProcessControlError("test-job-1", "start", "Process failed")
            
            with pytest.raises(ProcessControlError):
                queue.start_job("test-job-1")

    def test_stop_job_not_found(self):
        """Test stopping non-existent job."""
        queue = JobQueue()
        
        with pytest.raises(JobNotFoundError):
            queue.stop_job("non-existent")

    def test_stop_job_not_running(self):
        """Test stopping job that is not running."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Should not raise exception
        queue.stop_job("test-job-1")

    def test_stop_job_success(self):
        """Test successful job stop."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Mock job as running
        job = queue._jobs["test-job-1"]
        job._process = Mock()
        job._process.is_alive.return_value = True
        
        # Mock job stop
        with patch.object(job, 'stop_process') as mock_stop:
            queue.stop_job("test-job-1")
            
            mock_stop.assert_called_once()

    def test_stop_job_failure(self):
        """Test job stop with failure."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Mock job as running
        job = queue._jobs["test-job-1"]
        job._process = Mock()
        job._process.is_alive.return_value = True
        
        # Mock job stop to raise exception
        with patch.object(job, 'stop_process') as mock_stop:
            mock_stop.side_effect = ProcessControlError("test-job-1", "stop", "Stop failed")
            
            with pytest.raises(ProcessControlError):
                queue.stop_job("test-job-1")

    def test_suspend_job(self):
        """Test suspending job."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Mock job stop
        with patch.object(queue, 'stop_job') as mock_stop:
            queue.suspend_job("test-job-1")
            
            mock_stop.assert_called_once_with("test-job-1")

    def test_cleanup_completed_jobs(self):
        """Test cleaning up completed jobs."""
        queue = JobQueue()
        
        # Add test jobs
        queue.add_job(TestJob, "test-job-1", {})
        queue.add_job(TestJob, "test-job-2", {})
        
        # Mock jobs as completed
        job1 = queue._jobs["test-job-1"]
        job1._shared_state = {"status": Mock()}
        job1._shared_state["status"].value = JobStatus.COMPLETED.value
        
        job2 = queue._jobs["test-job-2"]
        job2._shared_state = {"status": Mock()}
        job2._shared_state["status"].value = JobStatus.ERROR.value
        
        # Mock get_status
        with patch.object(job1, 'get_status') as mock_get_status1:
            with patch.object(job2, 'get_status') as mock_get_status2:
                mock_get_status1.return_value = {"status": JobStatus.COMPLETED}
                mock_get_status2.return_value = {"status": JobStatus.ERROR}
                
                removed_count = queue.cleanup_completed_jobs()
                
                assert removed_count == 2
                assert "test-job-1" not in queue._jobs
                assert "test-job-2" not in queue._jobs

    def test_shutdown(self):
        """Test queue shutdown."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Mock job as running
        job = queue._jobs["test-job-1"]
        job._process = Mock()
        job._process.is_alive.return_value = True
        
        # Mock job stop
        with patch.object(job, 'stop_process') as mock_stop:
            queue.shutdown()
            
            mock_stop.assert_called_once()

    def test_shutdown_with_terminate(self):
        """Test queue shutdown with force terminate."""
        queue = JobQueue()
        
        # Add test job
        queue.add_job(TestJob, "test-job-1", {})
        
        # Mock job as running
        job = queue._jobs["test-job-1"]
        job._process = Mock()
        job._process.is_alive.return_value = True
        
        # Mock job stop to raise exception
        with patch.object(job, 'stop_process') as mock_stop:
            mock_stop.side_effect = ProcessControlError("test-job-1", "stop", "Stop failed")
            
            # Mock job terminate
            with patch.object(job, 'terminate_process') as mock_terminate:
                queue.shutdown()
                
                mock_stop.assert_called_once()
                mock_terminate.assert_called_once()
