"""
Tests for QueueJobBase initialization and basic functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import pytest
from unittest.mock import Mock, patch

from queuemgr.jobs.base import QueueJobBase
from queuemgr.exceptions import ValidationError


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


class TestQueueJobBaseInitialization:
    """Test QueueJobBase initialization."""

    def test_job_initialization(self):
        """Test basic job initialization."""
        job = TestJob("test-job-1", {"param1": "value1"})
        
        assert job.job_id == "test-job-1"
        assert job.params == {"param1": "value1"}
        assert job._registry is None
        assert job._shared_state is None
        assert job._process is None
        assert job.error is None

    def test_job_initialization_invalid_job_id(self):
        """Test job initialization with invalid job ID."""
        with pytest.raises(ValidationError):
            TestJob("", {"param1": "value1"})
        
        with pytest.raises(ValidationError):
            TestJob(None, {"param1": "value1"})

    def test_set_registry(self):
        """Test setting registry."""
        job = TestJob("test-job-1", {})
        registry = Mock()
        
        job._set_registry(registry)
        assert job._registry == registry

    def test_set_shared_state(self):
        """Test setting shared state."""
        job = TestJob("test-job-1", {})
        shared_state = {"status": Mock(), "command": Mock()}
        
        job._set_shared_state(shared_state)
        assert job._shared_state == shared_state

    def test_get_status_no_shared_state(self):
        """Test getting status without shared state."""
        job = TestJob("test-job-1", {})
        
        status = job.get_status()
        assert status["status"].name == "PENDING"
        assert status["command"].name == "NONE"
        assert status["progress"] == 0
        assert status["description"] == ""
        assert status["result"] is None

    def test_get_status_with_shared_state(self):
        """Test getting status with shared state."""
        job = TestJob("test-job-1", {})
        
        # Mock shared state
        mock_status = Mock()
        mock_status.value = 2  # RUNNING
        mock_command = Mock()
        mock_command.value = 1  # START
        mock_progress = Mock()
        mock_progress.value = 50
        mock_description = Mock()
        mock_description.value = b"Test description"
        mock_result = Mock()
        mock_result.value = {"test": "result"}
        
        shared_state = {
            "status": mock_status,
            "command": mock_command,
            "progress": mock_progress,
            "description": mock_description,
            "result": mock_result
        }
        
        job._set_shared_state(shared_state)
        
        status = job.get_status()
        assert status["status"].name == "RUNNING"
        assert status["command"].name == "START"
        assert status["progress"] == 50
        assert status["description"] == "Test description"
        assert status["result"] == {"test": "result"}

    def test_is_running_no_process(self):
        """Test is_running without process."""
        job = TestJob("test-job-1", {})
        assert not job.is_running()

    def test_is_running_with_process(self):
        """Test is_running with process."""
        job = TestJob("test-job-1", {})
        
        # Mock process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process
        
        assert job.is_running()
        
        # Test when process is not alive
        mock_process.is_alive.return_value = False
        assert not job.is_running()
