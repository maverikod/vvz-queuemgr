"""
Tests for custom exceptions.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from queuemgr.exceptions import (
    QueueManagerError,
    JobNotFoundError,
    JobAlreadyExistsError,
    InvalidJobStateError,
    JobExecutionError,
    RegistryError,
    ProcessControlError,
    ValidationError,
    TimeoutError,
)


class TestQueueManagerError:
    """Test base QueueManagerError."""

    def test_base_error_creation(self):
        """Test creating base error."""
        error = QueueManagerError("Test error")
        assert str(error) == "Test error"
        assert isinstance(error, Exception)


class TestJobNotFoundError:
    """Test JobNotFoundError."""

    def test_job_not_found_error_creation(self):
        """Test creating JobNotFoundError."""
        error = JobNotFoundError("test-job-123")
        assert error.job_id == "test-job-123"
        assert str(error) == "Job with ID 'test-job-123' not found"
        assert isinstance(error, QueueManagerError)


class TestJobAlreadyExistsError:
    """Test JobAlreadyExistsError."""

    def test_job_already_exists_error_creation(self):
        """Test creating JobAlreadyExistsError."""
        error = JobAlreadyExistsError("test-job-123")
        assert error.job_id == "test-job-123"
        assert str(error) == "Job with ID 'test-job-123' already exists"
        assert isinstance(error, QueueManagerError)


class TestInvalidJobStateError:
    """Test InvalidJobStateError."""

    def test_invalid_job_state_error_creation(self):
        """Test creating InvalidJobStateError."""
        error = InvalidJobStateError("test-job-123", "RUNNING", "start")
        assert error.job_id == "test-job-123"
        assert error.current_status == "RUNNING"
        assert error.operation == "start"
        assert str(error) == "Cannot start job 'test-job-123' in state 'RUNNING'"
        assert isinstance(error, QueueManagerError)


class TestJobExecutionError:
    """Test JobExecutionError."""

    def test_job_execution_error_creation(self):
        """Test creating JobExecutionError."""
        error = JobExecutionError("test-job-123")
        assert error.job_id == "test-job-123"
        assert error.original_error is None
        assert str(error) == "Job 'test-job-123' execution failed"
        assert isinstance(error, QueueManagerError)

    def test_job_execution_error_with_original(self):
        """Test creating JobExecutionError with original error."""
        original = ValueError("Original error")
        error = JobExecutionError("test-job-123", original)
        assert error.job_id == "test-job-123"
        assert error.original_error == original
        assert str(error) == "Job 'test-job-123' execution failed: Original error"
        assert isinstance(error, QueueManagerError)


class TestRegistryError:
    """Test RegistryError."""

    def test_registry_error_creation(self):
        """Test creating RegistryError."""
        error = RegistryError("Test registry error")
        assert error.original_error is None
        assert str(error) == "Registry error: Test registry error"
        assert isinstance(error, QueueManagerError)

    def test_registry_error_with_original(self):
        """Test creating RegistryError with original error."""
        original = IOError("File not found")
        error = RegistryError("Test registry error", original)
        assert error.original_error == original
        assert str(error) == "Registry error: Test registry error"
        assert isinstance(error, QueueManagerError)


class TestProcessControlError:
    """Test ProcessControlError."""

    def test_process_control_error_creation(self):
        """Test creating ProcessControlError."""
        error = ProcessControlError("test-job-123", "start")
        assert error.job_id == "test-job-123"
        assert error.operation == "start"
        assert error.original_error is None
        assert str(error) == "Process control error for job 'test-job-123' during start"
        assert isinstance(error, QueueManagerError)

    def test_process_control_error_with_original(self):
        """Test creating ProcessControlError with original error."""
        original = OSError("Process not found")
        error = ProcessControlError("test-job-123", "stop", original)
        assert error.job_id == "test-job-123"
        assert error.operation == "stop"
        assert error.original_error == original
        assert (
            str(error)
            == "Process control error for job 'test-job-123' during stop: Process not found"
        )
        assert isinstance(error, QueueManagerError)


class TestValidationError:
    """Test ValidationError."""

    def test_validation_error_creation(self):
        """Test creating ValidationError."""
        error = ValidationError("job_id", "", "must be non-empty")
        assert error.field == "job_id"
        assert error.value == ""
        assert error.reason == "must be non-empty"
        assert str(error) == "Validation error for field 'job_id': must be non-empty"
        assert isinstance(error, QueueManagerError)


class TestTimeoutError:
    """Test TimeoutError."""

    def test_timeout_error_creation(self):
        """Test creating TimeoutError."""
        error = TimeoutError("job_start", 30.0)
        assert error.operation == "job_start"
        assert error.timeout_seconds == 30.0
        assert str(error) == "Operation 'job_start' timed out after 30.0 seconds"
        assert isinstance(error, QueueManagerError)


class TestExceptionHierarchy:
    """Test exception hierarchy."""

    def test_exception_inheritance(self):
        """Test that all exceptions inherit from QueueManagerError."""
        exceptions = [
            JobNotFoundError("test"),
            JobAlreadyExistsError("test"),
            InvalidJobStateError("test", "RUNNING", "start"),
            JobExecutionError("test"),
            RegistryError("test"),
            ProcessControlError("test", "start"),
            ValidationError("field", "value", "reason"),
            TimeoutError("operation", 10.0),
        ]

        for exc in exceptions:
            assert isinstance(exc, QueueManagerError)
            assert isinstance(exc, Exception)

    def test_exception_chaining(self):
        """Test exception chaining."""
        original = ValueError("Original error")
        error = JobExecutionError("test-job", original)

        assert error.original_error == original
        assert error.__cause__ is None  # Not using __cause__ for chaining
        assert error.__context__ is None  # Not using __context__ for chaining
