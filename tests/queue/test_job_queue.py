"""
Tests for job queue implementation.


Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import pytest
import time
from unittest.mock import Mock, patch
from datetime import datetime
from queuemgr.queue.job_queue import JobQueue
from queuemgr.core.registry import JsonlRegistry
from queuemgr.core.types import JobStatus, JobCommand
from queuemgr.jobs.base import QueueJobBase
from queuemgr.exceptions import (
    JobNotFoundError,
    JobAlreadyExistsError,
    InvalidJobStateError,
    ProcessControlError
)


class TestJob(QueueJobBase):
    """Test implementation of QueueJobBase."""
    
    def __init__(self, job_id: str, params: dict):
        super().__init__(job_id, params)
        self.executed = False
        self.started = False
        self.stopped = False
        self.ended = False
        self.errored = False
        self.error = None
    
    def execute(self) -> None:
        """Test execute method."""
        self.executed = True
        time.sleep(0.1)  # Simulate work
    
    def on_start(self) -> None:
        """Test on_start method."""
        self.started = True
    
    def on_stop(self) -> None:
        """Test on_stop method."""
        self.stopped = True
    
    def on_end(self) -> None:
        """Test on_end method."""
        self.ended = True
    
    def on_error(self, exc: BaseException) -> None:
        """Test on_error method."""
        self.errored = True
        self.error = exc


class TestJobQueue:
    """Test JobQueue functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.registry = Mock(spec=JsonlRegistry)
        self.queue = JobQueue(self.registry)
    
    def test_queue_initialization(self):
        """Test queue initialization."""
        assert self.queue.registry == self.registry
        assert len(self.queue._jobs) == 0
        assert len(self.queue._job_creation_times) == 0
        assert self.queue._manager is not None
    
    def test_get_jobs_empty(self):
        """Test getting jobs when queue is empty."""
        jobs = self.queue.get_jobs()
        assert len(jobs) == 0
        assert isinstance(jobs, dict)
    
    def test_get_jobs_with_jobs(self):
        """Test getting jobs when queue has jobs."""
        job1 = TestJob("job-1", {})
        job2 = TestJob("job-2", {})
        
        self.queue._jobs["job-1"] = job1
        self.queue._jobs["job-2"] = job2
        
        jobs = self.queue.get_jobs()
        assert len(jobs) == 2
        assert "job-1" in jobs
        assert "job-2" in jobs
        assert jobs["job-1"] == job1
        assert jobs["job-2"] == job2
    
    def test_get_job_status_not_found(self):
        """Test getting job status when job not found."""
        with pytest.raises(JobNotFoundError, match="Job with ID 'nonexistent' not found"):
            self.queue.get_job_status("nonexistent")
    
    def test_get_job_status_found(self):
        """Test getting job status when job is found."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        self.queue._job_creation_times["job-1"] = datetime.now()
        
        # Mock job status
        with patch.object(job, 'get_status', return_value={
            'status': JobStatus.RUNNING,
            'command': JobCommand.START,
            'progress': 50,
            'description': "Running",
            'result': {"key": "value"}
        }):
            status = self.queue.get_job_status("job-1")
            
            assert status.job_id == "job-1"
            assert status.status == JobStatus.RUNNING
            assert status.progress == 50
            assert status.description == "Running"
            assert status.result == {"key": "value"}
    
    def test_add_job_success(self):
        """Test adding a job successfully."""
        job = TestJob("job-1", {"param": "value"})
        
        with patch('queuemgr.queue.job_queue.create_job_shared_state') as mock_create_shared, \
             patch('queuemgr.queue.job_queue.JobRecord') as mock_record_class:
            
            mock_shared_state = {"status": "test"}
            mock_create_shared.return_value = mock_shared_state
            
            job_id = self.queue.add_job(job)
            
            assert job_id == "job-1"
            assert "job-1" in self.queue._jobs
            assert self.queue._jobs["job-1"] == job
            assert "job-1" in self.queue._job_creation_times
            
            # Check that shared state was set
            assert job._shared_state == mock_shared_state
            assert job._registry == self.registry
            
            # Check that initial record was appended
            self.registry.append.assert_called_once()
    
    def test_add_job_already_exists(self):
        """Test adding a job that already exists."""
        job1 = TestJob("job-1", {})
        job2 = TestJob("job-1", {})
        
        self.queue._jobs["job-1"] = job1
        
        with pytest.raises(JobAlreadyExistsError, match="Job with ID 'job-1' already exists"):
            self.queue.add_job(job2)
    
    def test_delete_job_not_found(self):
        """Test deleting a job that doesn't exist."""
        with pytest.raises(JobNotFoundError, match="Job with ID 'nonexistent' not found"):
            self.queue.delete_job("nonexistent")
    
    def test_delete_job_not_running(self):
        """Test deleting a job that's not running."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        self.queue._job_creation_times["job-1"] = datetime.now()
        
        with patch.object(job, 'is_running', return_value=False):
            self.queue.delete_job("job-1")
            
            assert "job-1" not in self.queue._jobs
            assert "job-1" not in self.queue._job_creation_times
            self.registry.append.assert_called_once()
    
    def test_delete_job_running_graceful(self):
        """Test deleting a running job gracefully."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        self.queue._job_creation_times["job-1"] = datetime.now()
        
        with patch.object(job, 'is_running', return_value=True), \
             patch.object(job, 'stop_process') as mock_stop:
            
            self.queue.delete_job("job-1", force=False)
            
            mock_stop.assert_called_once_with(timeout=10.0)
            assert "job-1" not in self.queue._jobs
            assert "job-1" not in self.queue._job_creation_times
            self.registry.append.assert_called_once()
    
    def test_delete_job_running_force(self):
        """Test force deleting a running job."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        self.queue._job_creation_times["job-1"] = datetime.now()
        
        with patch.object(job, 'is_running', return_value=True), \
             patch.object(job, 'terminate_process') as mock_terminate:
            
            self.queue.delete_job("job-1", force=True)
            
            mock_terminate.assert_called_once()
            assert "job-1" not in self.queue._jobs
            assert "job-1" not in self.queue._job_creation_times
            self.registry.append.assert_called_once()
    
    def test_delete_job_stop_failure(self):
        """Test deleting a job when stop fails."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        self.queue._job_creation_times["job-1"] = datetime.now()
        
        with patch.object(job, 'is_running', return_value=True), \
             patch.object(job, 'stop_process', side_effect=ProcessControlError("job-1", "stop", Exception("Stop failed"))), \
             patch.object(job, 'terminate_process') as mock_terminate:
            
            # Should raise exception when force=False and stop fails
            with pytest.raises(ProcessControlError, match="Process control error for job 'job-1' during stop"):
                self.queue.delete_job("job-1", force=False)
            
            # terminate should not be called when force=False
            mock_terminate.assert_not_called()
            # Job should not be deleted
            assert "job-1" in self.queue._jobs
            assert "job-1" in self.queue._job_creation_times
    
    def test_start_job_not_found(self):
        """Test starting a job that doesn't exist."""
        with pytest.raises(JobNotFoundError, match="Job with ID 'nonexistent' not found"):
            self.queue.start_job("nonexistent")
    
    def test_start_job_invalid_state(self):
        """Test starting a job in invalid state."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        
        with patch.object(job, 'get_status', return_value={'status': JobStatus.RUNNING}), \
             patch.object(job, 'is_running', return_value=True):
            
            with pytest.raises(InvalidJobStateError, match="Cannot start job 'job-1' in state 'RUNNING'"):
                self.queue.start_job("job-1")
    
    def test_start_job_already_running(self):
        """Test starting a job that's already running."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        
        with patch.object(job, 'get_status', return_value={'status': JobStatus.RUNNING}), \
             patch.object(job, 'is_running', return_value=True):
            
            with pytest.raises(InvalidJobStateError, match="Cannot start job 'job-1' in state 'RUNNING'"):
                self.queue.start_job("job-1")
    
    def test_start_job_success(self):
        """Test starting a job successfully."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        
        with patch.object(job, 'get_status', return_value={'status': JobStatus.PENDING}), \
             patch.object(job, 'is_running', return_value=False), \
             patch.object(job, 'start_process') as mock_start:
            
            self.queue.start_job("job-1")
            mock_start.assert_called_once()
    
    def test_start_job_process_failure(self):
        """Test starting a job when process creation fails."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        
        with patch.object(job, 'get_status', return_value={'status': JobStatus.PENDING}), \
             patch.object(job, 'is_running', return_value=False), \
             patch.object(job, 'start_process', side_effect=ProcessControlError("job-1", "start", Exception("Process failed"))):
            
            with pytest.raises(ProcessControlError, match="Process failed"):
                self.queue.start_job("job-1")
    
    def test_stop_job_not_found(self):
        """Test stopping a job that doesn't exist."""
        with pytest.raises(JobNotFoundError, match="Job with ID 'nonexistent' not found"):
            self.queue.stop_job("nonexistent")
    
    def test_stop_job_not_running(self):
        """Test stopping a job that's not running."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        
        with patch.object(job, 'is_running', return_value=False):
            # Should not raise error when job is not running
            self.queue.stop_job("job-1")
    
    def test_stop_job_success(self):
        """Test stopping a job successfully."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        
        with patch.object(job, 'is_running', return_value=True), \
             patch.object(job, 'stop_process') as mock_stop:
            
            self.queue.stop_job("job-1", timeout=5.0)
            mock_stop.assert_called_once_with(timeout=5.0)
    
    def test_stop_job_failure(self):
        """Test stopping a job when stop fails."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        
        with patch.object(job, 'is_running', return_value=True), \
             patch.object(job, 'stop_process', side_effect=ProcessControlError("job-1", "stop", Exception("Stop failed"))):
            
            with pytest.raises(ProcessControlError, match="Stop failed"):
                self.queue.stop_job("job-1")
    
    def test_suspend_job(self):
        """Test suspending a job."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        
        with patch.object(self.queue, 'stop_job') as mock_stop:
            self.queue.suspend_job("job-1")
            mock_stop.assert_called_once_with("job-1")
    
    def test_get_job_count(self):
        """Test getting job count."""
        assert self.queue.get_job_count() == 0
        
        self.queue._jobs["job-1"] = TestJob("job-1", {})
        assert self.queue.get_job_count() == 1
        
        self.queue._jobs["job-2"] = TestJob("job-2", {})
        assert self.queue.get_job_count() == 2
    
    def test_get_running_jobs(self):
        """Test getting running jobs."""
        job1 = TestJob("job-1", {})
        job2 = TestJob("job-2", {})
        job3 = TestJob("job-3", {})
        
        self.queue._jobs["job-1"] = job1
        self.queue._jobs["job-2"] = job2
        self.queue._jobs["job-3"] = job3
        
        with patch.object(job1, 'is_running', return_value=True), \
             patch.object(job2, 'is_running', return_value=False), \
             patch.object(job3, 'is_running', return_value=True):
            
            running_jobs = self.queue.get_running_jobs()
            assert len(running_jobs) == 2
            assert "job-1" in running_jobs
            assert "job-3" in running_jobs
            assert "job-2" not in running_jobs
    
    def test_get_job_by_id(self):
        """Test getting job by ID."""
        job = TestJob("job-1", {})
        self.queue._jobs["job-1"] = job
        
        assert self.queue.get_job_by_id("job-1") == job
        assert self.queue.get_job_by_id("nonexistent") is None
    
    def test_list_job_statuses(self):
        """Test listing job statuses."""
        job1 = TestJob("job-1", {})
        job2 = TestJob("job-2", {})
        
        self.queue._jobs["job-1"] = job1
        self.queue._jobs["job-2"] = job2
        
        with patch.object(job1, 'get_status', return_value={'status': JobStatus.RUNNING}), \
             patch.object(job2, 'get_status', return_value={'status': JobStatus.COMPLETED}):
            
            statuses = self.queue.list_job_statuses()
            assert statuses["job-1"] == JobStatus.RUNNING
            assert statuses["job-2"] == JobStatus.COMPLETED
    
    def test_cleanup_completed_jobs(self):
        """Test cleaning up completed jobs."""
        job1 = TestJob("job-1", {})
        job2 = TestJob("job-2", {})
        job3 = TestJob("job-3", {})
        
        self.queue._jobs["job-1"] = job1
        self.queue._jobs["job-2"] = job2
        self.queue._jobs["job-3"] = job3
        
        self.queue._job_creation_times["job-1"] = datetime.now()
        self.queue._job_creation_times["job-2"] = datetime.now()
        self.queue._job_creation_times["job-3"] = datetime.now()
        
        with patch.object(job1, 'get_status', return_value={'status': JobStatus.COMPLETED}), \
             patch.object(job2, 'get_status', return_value={'status': JobStatus.ERROR}), \
             patch.object(job3, 'get_status', return_value={'status': JobStatus.RUNNING}):
            
            removed_count = self.queue.cleanup_completed_jobs()
            assert removed_count == 2
            assert "job-1" not in self.queue._jobs
            assert "job-2" not in self.queue._jobs
            assert "job-3" in self.queue._jobs
            assert "job-1" not in self.queue._job_creation_times
            assert "job-2" not in self.queue._job_creation_times
            assert "job-3" in self.queue._job_creation_times
    
    def test_shutdown(self):
        """Test shutting down the queue."""
        job1 = TestJob("job-1", {})
        job2 = TestJob("job-2", {})
        
        self.queue._jobs["job-1"] = job1
        self.queue._jobs["job-2"] = job2
        
        with patch.object(self.queue, 'get_running_jobs', return_value={"job-1": job1}), \
             patch.object(job1, 'stop_process') as mock_stop:
            
            self.queue.shutdown(timeout=5.0)
            
            mock_stop.assert_called_once_with(timeout=5.0)
            assert len(self.queue._jobs) == 0
            assert len(self.queue._job_creation_times) == 0
    
    def test_shutdown_with_terminate(self):
        """Test shutting down the queue with process termination."""
        job1 = TestJob("job-1", {})
        job2 = TestJob("job-2", {})
        
        self.queue._jobs["job-1"] = job1
        self.queue._jobs["job-2"] = job2
        
        with patch.object(self.queue, 'get_running_jobs', return_value={"job-1": job1}), \
             patch.object(job1, 'stop_process', side_effect=ProcessControlError("job-1", "stop", Exception("Stop failed"))), \
             patch.object(job1, 'terminate_process') as mock_terminate:
            
            self.queue.shutdown(timeout=5.0)
            
            mock_terminate.assert_called_once()
            assert len(self.queue._jobs) == 0
            assert len(self.queue._job_creation_times) == 0
