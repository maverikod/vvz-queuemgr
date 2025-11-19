"""
Placeholder job class for jobs loaded from registry.

This module provides a placeholder job class that can be used to restore
job metadata from registry records when the original job class is not available.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from typing import Dict, Any

from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.types import JobRecord


class RegistryPlaceholderJob(QueueJobBase):
    """
    Placeholder job class for jobs loaded from registry.

    This class is used to restore job metadata from registry records
    when the original job class is not available. It provides read-only
    access to job status and metadata, but cannot be executed.
    """

    def __init__(self, job_id: str, record: JobRecord) -> None:
        """
        Initialize placeholder job from registry record.

        Args:
            job_id: Job identifier.
            record: JobRecord containing job metadata.
        """
        super().__init__(job_id, {})
        self._record = record
        self._is_placeholder = True

    def execute(self) -> None:
        """
        Execute placeholder job (not supported).

        Raises:
            RuntimeError: Always, as placeholder jobs cannot be executed.
        """
        raise RuntimeError(
            f"Placeholder job {self.job_id} cannot be executed. "
            "Original job class is not available."
        )

    def get_status(self) -> Dict[str, Any]:
        """
        Get job status from registry record.

        Returns:
            Dictionary containing job status information from registry.
        """
        return {
            "status": self._record.status,
            "command": None,
            "progress": self._record.progress,
            "description": self._record.description,
            "result": self._record.result,
        }

    def is_running(self) -> bool:
        """
        Check if job is running (always False for placeholder jobs).

        Returns:
            False, as placeholder jobs cannot be running.
        """
        return False
