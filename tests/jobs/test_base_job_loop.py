"""
Tests for QueueJobBase job loop functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.types import JobCommand, JobStatus


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
        self._running = False
    
    def execute(self) -> None:
        """Execute the job."""
        pass


class TestQueueJobBaseJobLoop:
    """Test QueueJobBase job loop."""

    def test_job_loop_start_command(self):
        """Test job loop with START command."""
        job = TestJob("test-job-1", {})
        
        # Mock shared state
        mock_shared_state = {
            "status": Mock(),
            "command": Mock(),
            "progress": Mock(),
            "description": Mock(),
            "result": Mock()
        }
        job._set_shared_state(mock_shared_state)
        
        # Mock get_command to return START then NONE
        with patch('queuemgr.jobs.base.get_command') as mock_get_command:
            mock_get_command.side_effect = [JobCommand.START, JobCommand.NONE]
            
            # Mock execute
            with patch.object(job, 'execute') as mock_execute:
                job._job_loop()
                
                mock_execute.assert_called_once()

    def test_job_loop_stop_command(self):
        """Test job loop with STOP command."""
        job = TestJob("test-job-1", {})
        
        # Mock shared state
        mock_shared_state = {
            "status": Mock(),
            "command": Mock(),
            "progress": Mock(),
            "description": Mock(),
            "result": Mock()
        }
        job._set_shared_state(mock_shared_state)
        
        # Mock get_command to return STOP
        with patch('queuemgr.jobs.base.get_command') as mock_get_command:
            mock_get_command.return_value = JobCommand.STOP
            
            # Mock _handle_stop
            with patch.object(job, '_handle_stop') as mock_handle_stop:
                job._job_loop()
                
                mock_handle_stop.assert_called_once()

    def test_job_loop_delete_command(self):
        """Test job loop with DELETE command."""
        job = TestJob("test-job-1", {})
        
        # Mock shared state
        mock_shared_state = {
            "status": Mock(),
            "command": Mock(),
            "progress": Mock(),
            "description": Mock(),
            "result": Mock()
        }
        job._set_shared_state(mock_shared_state)
        
        # Mock get_command to return DELETE
        with patch('queuemgr.jobs.base.get_command') as mock_get_command:
            mock_get_command.return_value = JobCommand.DELETE
            
            # Mock _handle_delete
            with patch.object(job, '_handle_delete') as mock_handle_delete:
                job._job_loop()
                
                mock_handle_delete.assert_called_once()

    def test_job_loop_execution_error(self):
        """Test job loop with execution error."""
        job = TestJob("test-job-1", {})
        
        # Mock shared state
        mock_shared_state = {
            "status": Mock(),
            "command": Mock(),
            "progress": Mock(),
            "description": Mock(),
            "result": Mock()
        }
        job._set_shared_state(mock_shared_state)
        
        # Mock get_command to return START then NONE
        with patch('queuemgr.jobs.base.get_command') as mock_get_command:
            mock_get_command.side_effect = [JobCommand.START, JobCommand.NONE]
            
            # Mock execute to raise exception
            with patch.object(job, 'execute') as mock_execute:
                mock_execute.side_effect = ValueError("Test error")
                
                job._job_loop()
                
                assert job.error is not None

    def test_job_loop_stop_error(self):
        """Test job loop with stop error."""
        job = TestJob("test-job-1", {})
        
        # Mock shared state
        mock_shared_state = {
            "status": Mock(),
            "command": Mock(),
            "progress": Mock(),
            "description": Mock(),
            "result": Mock()
        }
        job._set_shared_state(mock_shared_state)
        
        # Mock get_command to return STOP
        with patch('queuemgr.jobs.base.get_command') as mock_get_command:
            mock_get_command.return_value = JobCommand.STOP
            
            # Mock _handle_stop to raise exception
            with patch.object(job, '_handle_stop') as mock_handle_stop:
                mock_handle_stop.side_effect = Exception("Stop error")
                
                job._job_loop()
                
                assert job.error is not None

    def test_job_loop_end_error(self):
        """Test job loop with end error."""
        job = TestJob("test-job-1", {})
        
        # Mock shared state
        mock_shared_state = {
            "status": Mock(),
            "command": Mock(),
            "progress": Mock(),
            "description": Mock(),
            "result": Mock()
        }
        job._set_shared_state(mock_shared_state)
        
        # Mock get_command to return START then NONE
        with patch('queuemgr.jobs.base.get_command') as mock_get_command:
            mock_get_command.side_effect = [JobCommand.START, JobCommand.NONE]
            
            # Mock _handle_completion to raise exception
            with patch.object(job, '_handle_completion') as mock_handle_completion:
                mock_handle_completion.side_effect = Exception("End error")
                
                job._job_loop()
                
                assert job.error is not None

    def test_job_loop_error_handler_error(self):
        """Test job loop with error handler error."""
        job = TestJob("test-job-1", {})
        
        # Mock shared state
        mock_shared_state = {
            "status": Mock(),
            "command": Mock(),
            "progress": Mock(),
            "description": Mock(),
            "result": Mock()
        }
        job._set_shared_state(mock_shared_state)
        
        # Mock get_command to return START then NONE
        with patch('queuemgr.jobs.base.get_command') as mock_get_command:
            mock_get_command.side_effect = [JobCommand.START, JobCommand.NONE]
            
            # Mock execute to raise exception
            with patch.object(job, 'execute') as mock_execute:
                mock_execute.side_effect = ValueError("Test error")
                
                # Mock _handle_error to raise exception
                with patch.object(job, '_handle_error') as mock_handle_error:
                    mock_handle_error.side_effect = Exception("Error handler error")
                    
                    # This should not raise exception
                    job._job_loop()
                    
                    assert job.error is not None
