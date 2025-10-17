"""
Core exceptions for IPC and registry operations.

This module contains specific exceptions for core functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from queuemgr.exceptions import QueueManagerError


class IPCError(QueueManagerError):
    """Raised when IPC operations fail."""

    def __init__(self, operation: str, message: str) -> None:
        """
        Initialize IPCError.

        Args:
            operation: The IPC operation that failed.
            message: Error message.
        """
        self.operation = operation
        super().__init__(f"IPC error during {operation}: {message}")


class RegistryError(QueueManagerError):
    """Raised when registry operations fail."""

    def __init__(self, message: str, original_error: Exception = None) -> None:
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
        self, job_id: str, operation: str, original_error: Exception = None
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
