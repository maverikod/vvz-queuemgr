"""
Job-specific exceptions.

This module contains exceptions specific to job operations.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from queuemgr.exceptions import QueueManagerError


class JobExecutionError(QueueManagerError):
    """Raised when a job fails during execution."""

    def __init__(self, job_id: str, original_error: Exception = None) -> None:
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


class ValidationError(QueueManagerError):
    """Raised when input validation fails."""

    def __init__(self, field: str, value, reason: str) -> None:
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
