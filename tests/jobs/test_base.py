"""
Tests for base job class.


Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import pytest
import time
from unittest.mock import Mock, patch
from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.types import JobStatus, JobCommand
from queuemgr.exceptions import ValidationError, ProcessControlError


def create_mock_shared_state():
    """Create a properly structured mock shared state for testing."""
    return {
        'status': Mock(value=JobStatus.PENDING),
        'command': Mock(value=JobCommand.NONE),
        'progress': Mock(value=0),
        'description': Mock(value=b""),
        'result': Mock(value=None),
        'lock': Mock()
    }


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


class TestQueueJobBase:
    """Test QueueJobBase functionality."""
    
    def test_job_initialization(self):
        """Test job initialization."""
        job = TestJob("test-job-123", {"param1": "value1"})
        
        assert job.job_id == "test-job-123"
        assert job.params == {"param1": "value1"}
        assert job._process is None
        assert job._shared_state is None
        assert job._registry is None
        assert job._running is False
    
    def test_job_initialization_invalid_job_id(self):
        """Test job initialization with invalid job_id."""
        with pytest.raises(ValidationError, match="must be a non-empty string"):
            TestJob("", {"param1": "value1"})
        
        with pytest.raises(ValidationError, match="must be a non-empty string"):
            TestJob(None, {"param1": "value1"})  # type: ignore
    
    def test_set_registry(self):
        """Test setting registry."""
        job = TestJob("test-job-123", {})
        registry = Mock()
        
        job._set_registry(registry)
        assert job._registry == registry
    
    def test_set_shared_state(self):
        """Test setting shared state."""
        job = TestJob("test-job-123", {})
        shared_state = {"status": "test"}
        
        job._set_shared_state(shared_state)
        assert job._shared_state == shared_state
    
    def test_get_status_no_shared_state(self):
        """Test getting status when no shared state is set."""
        job = TestJob("test-job-123", {})
        status = job.get_status()
        
        assert status['status'] == JobStatus.PENDING
        assert status['command'] == JobCommand.NONE
        assert status['progress'] == 0
        assert status['description'] == 'Job not started'
        assert status['result'] is None
    
    def test_get_status_with_shared_state(self):
        """Test getting status with shared state."""
        job = TestJob("test-job-123", {})
        
        # Mock shared state
        mock_shared_state = {
            'status': Mock(value=JobStatus.RUNNING),
            'command': Mock(value=JobCommand.START),
            'progress': Mock(value=50),
            'description': Mock(value=b"Running"),
            'result': Mock(value={"key": "value"})
        }
        
        with patch('queuemgr.jobs.base.read_job_state', return_value={
            'status': JobStatus.RUNNING,
            'command': JobCommand.START,
            'progress': 50,
            'description': "Running",
            'result': {"key": "value"}
        }):
            job._set_shared_state(mock_shared_state)
            status = job.get_status()
            
            assert status['status'] == JobStatus.RUNNING
            assert status['command'] == JobCommand.START
            assert status['progress'] == 50
            assert status['description'] == "Running"
            assert status['result'] == {"key": "value"}
    
    def test_is_running_no_process(self):
        """Test is_running when no process is set."""
        job = TestJob("test-job-123", {})
        assert not job.is_running()
    
    def test_is_running_with_process(self):
        """Test is_running with process."""
        job = TestJob("test-job-123", {})
        
        # Mock process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process
        
        assert job.is_running()
        
        # Test when process is not alive
        mock_process.is_alive.return_value = False
        assert not job.is_running()
    
    def test_start_process_success(self):
        """Test starting process successfully."""
        job = TestJob("test-job-123", {})
        
        with patch('queuemgr.jobs.base.Process') as mock_process_class:
            mock_process = Mock()
            mock_process_class.return_value = mock_process
            mock_process.is_alive.return_value = False
            
            process = job.start_process()
            
            assert process == mock_process
            assert job._process == mock_process
            mock_process_class.assert_called_once_with(target=job._job_loop, name="Job-test-job-123")
            mock_process.start.assert_called_once()
    
    def test_start_process_already_running(self):
        """Test starting process when already running."""
        job = TestJob("test-job-123", {})
        
        # Mock existing running process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process
        
        with pytest.raises(ProcessControlError, match="Process already running"):
            job.start_process()
    
    def test_start_process_failure(self):
        """Test starting process with failure."""
        job = TestJob("test-job-123", {})
        
        with patch('queuemgr.jobs.base.Process') as mock_process_class:
            mock_process_class.side_effect = OSError("Process creation failed")
            
            with pytest.raises(ProcessControlError, match="Process creation failed"):
                job.start_process()
    
    def test_stop_process_not_running(self):
        """Test stopping process when not running."""
        job = TestJob("test-job-123", {})
        
        # Should not raise error when process is not running
        job.stop_process()
    
    def test_stop_process_success(self):
        """Test stopping process successfully."""
        job = TestJob("test-job-123", {})
        
        # Mock running process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process
        
        # Mock shared state and IPC functions
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        with patch('queuemgr.jobs.base.set_command') as mock_set_command, \
             patch('queuemgr.jobs.base.get_command', return_value=JobCommand.STOP):
            
            job.stop_process()
            
            mock_set_command.assert_called_once_with(mock_shared_state, JobCommand.STOP)
            mock_process.join.assert_called_once()
    
    def test_stop_process_with_timeout(self):
        """Test stopping process with timeout."""
        job = TestJob("test-job-123", {})
        
        # Mock running process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        with patch('queuemgr.jobs.base.set_command'), \
             patch('queuemgr.jobs.base.get_command', return_value=JobCommand.STOP):
            
            with pytest.raises(ProcessControlError, match="Process did not stop within"):
                job.stop_process(timeout=5.0)
            mock_process.join.assert_called_once_with(timeout=5.0)
    
    def test_stop_process_timeout_exceeded(self):
        """Test stopping process when timeout is exceeded."""
        job = TestJob("test-job-123", {})
        
        # Mock running process that doesn't stop
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        with patch('queuemgr.jobs.base.set_command'), \
             patch('queuemgr.jobs.base.get_command', return_value=JobCommand.STOP):
            
            with pytest.raises(ProcessControlError, match="Process did not stop within"):
                job.stop_process(timeout=0.1)
    
    def test_terminate_process_not_running(self):
        """Test terminating process when not running."""
        job = TestJob("test-job-123", {})
        
        # Should not raise error when process is not running
        job.terminate_process()
    
    def test_terminate_process_success(self):
        """Test terminating process successfully."""
        job = TestJob("test-job-123", {})
        
        # Mock running process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        with patch('queuemgr.jobs.base.set_command') as mock_set_command:
            job.terminate_process()
            
            mock_set_command.assert_called_once_with(mock_shared_state, JobCommand.DELETE)
            mock_process.terminate.assert_called_once()
            # join is called twice: once with timeout, once without
            assert mock_process.join.call_count == 2
    
    def test_terminate_process_force_kill(self):
        """Test terminating process with force kill."""
        job = TestJob("test-job-123", {})
        
        # Mock running process that doesn't terminate
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        with patch('queuemgr.jobs.base.set_command'):
            job.terminate_process()
            
            mock_process.terminate.assert_called_once()
            # join is called twice: once with timeout, once without
            assert mock_process.join.call_count == 2
            mock_process.kill.assert_called_once()
    
    def test_terminate_process_failure(self):
        """Test terminating process with failure."""
        job = TestJob("test-job-123", {})
        
        # Mock running process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        with patch('queuemgr.jobs.base.set_command') as mock_set_command:
            mock_set_command.side_effect = OSError("Command failed")
            
            with pytest.raises(ProcessControlError, match="Command failed"):
                job.terminate_process()
    
    def test_job_loop_start_command(self):
        """Test job loop with START command."""
        job = TestJob("test-job-123", {})
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        # Mock get_command to return START first (for initial wait), then START again (for main loop), then STOP
        with patch('queuemgr.jobs.base.get_command', side_effect=[JobCommand.START, JobCommand.START, JobCommand.STOP]), \
             patch('queuemgr.jobs.base.update_job_state') as mock_update, \
             patch('queuemgr.jobs.base.read_job_state', return_value={'status': JobStatus.RUNNING}):
            
            job._job_loop()
            
            # Should call on_start
            assert job.started
            # Should call execute
            assert job.executed
            # Should call on_stop (because we sent STOP command)
            assert job.stopped
            # Should not call on_end (because we stopped, not completed)
            assert not job.ended
    
    def test_job_loop_stop_command(self):
        """Test job loop with STOP command."""
        job = TestJob("test-job-123", {})
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        with patch('queuemgr.jobs.base.get_command', side_effect=[JobCommand.START, JobCommand.STOP]), \
             patch('queuemgr.jobs.base.update_job_state') as mock_update, \
             patch('queuemgr.jobs.base.read_job_state', return_value={'status': JobStatus.RUNNING}):
            
            job._job_loop()
            
            # Should call on_start
            assert job.started
            # Should call on_stop
            assert job.stopped
            # Should not call on_end
            assert not job.ended
    
    def test_job_loop_delete_command(self):
        """Test job loop with DELETE command."""
        job = TestJob("test-job-123", {})
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        with patch('queuemgr.jobs.base.get_command', return_value=JobCommand.DELETE), \
             patch('queuemgr.jobs.base.update_job_state') as mock_update, \
             patch('queuemgr.jobs.base.read_job_state', return_value={'status': JobStatus.INTERRUPTED}):
            
            job._job_loop()
            
            # Should not call on_start
            assert not job.started
            # Should not call execute
            assert not job.executed
            # Should not call on_end
            assert not job.ended
    
    def test_job_loop_execution_error(self):
        """Test job loop with execution error."""
        job = TestJob("test-job-123", {})
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        # Override execute to raise exception
        def failing_execute():
            raise ValueError("Execution failed")
        
        job.execute = failing_execute
        
        with patch('queuemgr.jobs.base.get_command', return_value=JobCommand.START), \
             patch('queuemgr.jobs.base.update_job_state') as mock_update, \
             patch('queuemgr.jobs.base.read_job_state', return_value={'status': JobStatus.RUNNING}):
            
            job._job_loop()
            
            # Should call on_start
            assert job.started
            # Should call on_error
            assert job.errored
            assert isinstance(job.error, ValueError)
            assert str(job.error) == "Execution failed"
            # Should not call on_end
            assert not job.ended
    
    def test_job_loop_stop_error(self):
        """Test job loop with error during stop."""
        job = TestJob("test-job-123", {})
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        # Override on_stop to raise exception
        def failing_on_stop():
            job.stopped = True  # Mark as stopped before raising
            raise RuntimeError("Stop failed")
        
        job.on_stop = failing_on_stop
        
        with patch('queuemgr.jobs.base.get_command', side_effect=[JobCommand.START, JobCommand.STOP]), \
             patch('queuemgr.jobs.base.update_job_state') as mock_update, \
             patch('queuemgr.jobs.base.read_job_state', return_value={'status': JobStatus.RUNNING}):
            
            job._job_loop()
            
            # Should call on_start
            assert job.started
            # Should not call execute (because STOP command comes immediately)
            assert not job.executed
            # Should call on_stop (because we sent STOP command)
            assert job.stopped
            # Should not call on_end (because we stopped, not completed)
            assert not job.ended
    
    def test_job_loop_end_error(self):
        """Test job loop with error during end."""
        job = TestJob("test-job-123", {})
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        # Override execute to set _running to False after first call
        original_execute = job.execute
        def execute_once():
            original_execute()
            job._running = False  # Exit the loop after first execution
        
        job.execute = execute_once
        
        # Override on_end to raise exception
        def failing_on_end():
            raise RuntimeError("End failed")
        
        job.on_end = failing_on_end
        
        with patch('queuemgr.jobs.base.get_command', return_value=JobCommand.START), \
             patch('queuemgr.jobs.base.update_job_state') as mock_update, \
             patch('queuemgr.jobs.base.read_job_state', return_value={'status': JobStatus.RUNNING}):
            
            job._job_loop()
            
            # Should call on_start
            assert job.started
            # Should call execute
            assert job.executed
            # Should not call on_end (because _running was False when loop ended)
            assert not job.ended
    
    def test_job_loop_error_handler_error(self):
        """Test job loop with error in error handler."""
        job = TestJob("test-job-123", {})
        
        # Mock shared state
        mock_shared_state = create_mock_shared_state()
        job._set_shared_state(mock_shared_state)
        
        # Override execute to raise exception
        def failing_execute():
            job.executed = True  # Mark as executed before raising
            raise ValueError("Execution failed")
        
        job.execute = failing_execute
        
        # Override on_error to save error and raise exception
        def failing_on_error(exc):
            job.errored = True
            job.error = exc
            raise RuntimeError("Error handler failed")
        
        job.on_error = failing_on_error
        
        with patch('queuemgr.jobs.base.get_command', return_value=JobCommand.START), \
             patch('queuemgr.jobs.base.update_job_state') as mock_update, \
             patch('queuemgr.jobs.base.read_job_state', return_value={'status': JobStatus.RUNNING}):
            
            # Should not raise exception even if error handler fails
            job._job_loop()
            
            # Should call on_start
            assert job.started
            # Should call on_error (even though it fails)
            assert job.errored
            assert isinstance(job.error, ValueError)
            assert str(job.error) == "Execution failed"
            # Should not call on_end (because error occurred)
            assert not job.ended
