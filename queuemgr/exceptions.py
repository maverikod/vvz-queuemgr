"""
Custom exceptions for the queue manager system.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from typing import Any, Optional, Union, Dict, List


class QueueManagerError(Exception):
    """Base exception for all queue manager errors."""

    pass


class JobNotFoundError(QueueManagerError):
    """Raised when a job with the specified ID is not found."""

    def __init__(self, job_id: str) -> None:
        """
        Initialize JobNotFoundError.

        Args:
            job_id: The job ID that was not found.
        """
        self.job_id = job_id
        super().__init__(f"Job with ID '{job_id}' not found")


class JobAlreadyExistsError(QueueManagerError):
    """Raised when trying to add a job that already exists."""

    def __init__(self, job_id: str) -> None:
        """
        Initialize JobAlreadyExistsError.

        Args:
            job_id: The job ID that already exists.
        """
        self.job_id = job_id
        super().__init__(f"Job with ID '{job_id}' already exists")


class InvalidJobStateError(QueueManagerError):
    """Raised when an operation is not valid for the current job state."""

    def __init__(self, job_id: str, current_status: str, operation: str) -> None:
        """
        Initialize InvalidJobStateError.

        Args:
            job_id: The job ID.
            current_status: The current job status.
            operation: The operation that was attempted.
        """
        self.job_id = job_id
        self.current_status = current_status
        self.operation = operation
        super().__init__(
            f"Cannot {operation} job '{job_id}' in state '{current_status}'"
        )


class JobExecutionError(QueueManagerError):
    """Raised when a job fails during execution."""

    def __init__(self, job_id: str, original_error: Optional[Exception] = None) -> None:
        """
        Initialize JobExecutionError.

        Args:
            job_id: The job ID that failed.
            original_error: The original exception that caused the failure.
        """
        self.job_id = job_id
        self.original_error = original_error
        error_msg = f"Job '{job_id}' execution failed"
        if original_error:
            error_msg += f": {original_error}"
        super().__init__(error_msg)


class RegistryError(QueueManagerError):
    """Raised when registry operations fail."""

    def __init__(
        self, message: str, original_error: Optional[Exception] = None
    ) -> None:
        """
        Initialize RegistryError.

        Args:
            message: Error message.
            original_error: The original exception that caused the failure.
        """
        self.original_error = original_error
        super().__init__(f"Registry error: {message}")


class ProcessControlError(QueueManagerError):
    """Raised when process control operations fail."""

    def __init__(
        self,
        job_id: str,
        operation: str,
        original_error: Optional[Exception] = None,
    ) -> None:
        """
        Initialize ProcessControlError.

        Args:
            job_id: The job ID.
            operation: The operation that failed.
            original_error: The original exception that caused the failure.
        """
        self.job_id = job_id
        self.operation = operation
        self.original_error = original_error
        error_msg = f"Process control error for job '{job_id}' during {operation}"
        if original_error:
            error_msg += f": {original_error}"
        super().__init__(error_msg)


class ValidationError(QueueManagerError):
    """Raised when input validation fails."""

    def __init__(
        self,
        field: str,
        value: Union[str, int, float, bool, Dict[str, Any], List[Any], None],
        reason: str,
    ) -> None:
        """
        Initialize ValidationError.

        Args:
            field: The field that failed validation.
            value: The value that failed validation.
            reason: The reason for validation failure.
        """
        self.field = field
        self.value = value
        self.reason = reason
        super().__init__(f"Validation error for field '{field}': {reason}")


class TimeoutError(QueueManagerError):
    """Raised when an operation times out."""

    def __init__(self, operation: str, timeout_seconds: float) -> None:
        """
        Initialize TimeoutError.

        Args:
            operation: The operation that timed out.
            timeout_seconds: The timeout duration in seconds.
        """
        self.operation = operation
        self.timeout_seconds = timeout_seconds
        super().__init__(
            f"Operation '{operation}' timed out after " f"{timeout_seconds} seconds"
        )
