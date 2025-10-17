"""
Queue-specific exceptions.

This module contains exceptions specific to queue operations.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from queuemgr.exceptions import QueueManagerError


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
