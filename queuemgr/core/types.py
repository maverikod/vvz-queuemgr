"""
Core types and enums for the queue manager system.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from typing import Any, Optional


class JobStatus(IntEnum):
    """Job execution status."""

    PENDING = 0
    RUNNING = 1
    COMPLETED = 2
    ERROR = 3
    INTERRUPTED = 4


class JobCommand(IntEnum):
    """Commands that can be sent to a job."""

    NONE = 0
    START = 1
    STOP = 2
    DELETE = 3


# Type aliases
JobId = str
JobResult = Any


@dataclass(frozen=True)
class JobRecord:
    """
    Immutable record representing a job's state at a point in time.

    Attributes:
        job_id: Unique identifier for the job
        status: Current execution status
        progress: Progress percentage (0-100)
        description: Human-readable description of current state
        result: Job result data (if any)
        created_at: When the job was created
        updated_at: When this record was created
    """

    job_id: JobId
    status: JobStatus
    progress: int
    description: Optional[str]
    result: Optional[JobResult]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        """Validate record data after initialization."""
        if not isinstance(self.job_id, str) or not self.job_id:
            raise ValueError("job_id must be a non-empty string")

        if not isinstance(self.progress, int) or not (0 <= self.progress <= 100):
            raise ValueError("progress must be an integer between 0 and 100")

        if not isinstance(self.status, JobStatus):
            raise ValueError("status must be a JobStatus enum value")

        if self.created_at > self.updated_at:
            raise ValueError("created_at must be before or equal to updated_at")
