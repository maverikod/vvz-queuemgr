"""
Command implementations for the MCP adapter example.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, Optional, Type

from mcp_proxy_adapter.commands.base import Command
from mcp_proxy_adapter.core.errors import MicroserviceError, ValidationError

from queuemgr.async_simple_api import AsyncQueueSystem
from queuemgr.jobs.base import QueueJobBase

QueueAccessor = Callable[[], Optional[AsyncQueueSystem]]
JobClassMap = Dict[str, Type[QueueJobBase]]


class _QueueCommandBase(Command):
    """
    Base command that provides access to the shared queue system.

    Args:
        queue_accessor: Callable returning the active AsyncQueueSystem instance.
    """

    def __init__(self, queue_accessor: QueueAccessor) -> None:
        """
        Initialize the shared command base.

        Args:
            queue_accessor: Callable returning the active queue instance. The callable
                must not block and should return ``None`` when the queue is unavailable.
        """
        super().__init__()
        self._queue_accessor = queue_accessor

    def _get_queue(self) -> AsyncQueueSystem:
        """
        Get an active queue system instance or raise an error.

        Returns:
            AsyncQueueSystem: Active queue system instance.

        Raises:
            MicroserviceError: If the queue system is not running.
        """
        queue = self._queue_accessor()
        if not queue or not queue.is_running():
            raise MicroserviceError("Queue system is not running")
        return queue


class QueueAddJobCommand(_QueueCommandBase):
    """
    Command to add a job to the queue.

    Args:
        queue_accessor: Callable returning the queue instance.
        job_classes: Mapping of job type names to job classes.
    """

    def __init__(self, queue_accessor: QueueAccessor, job_classes: JobClassMap) -> None:
        """
        Configure the add-job command metadata and dependencies.

        Args:
            queue_accessor: Callable that yields the active queue system.
            job_classes: Mapping of job type names to concrete job classes supported
                by the adapter.
        """
        super().__init__(queue_accessor)
        self.name = "queue_add_job"
        self.description = "Add a job to the queue"
        self.version = "1.0.0"
        self._job_classes = job_classes

    def get_schema(self) -> Dict[str, Any]:
        """Describe the schema for the add job command."""
        return {
            "type": "object",
            "properties": {
                "job_type": {
                    "type": "string",
                    "enum": list(self._job_classes.keys()),
                    "description": "Type of job to add",
                },
                "job_id": {"type": "string", "description": "Unique job identifier"},
                "params": {"type": "object", "description": "Job parameters"},
            },
            "required": ["job_type", "job_id", "params"],
        }

    async def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute queue add job command."""
        queue = self._get_queue()
        job_type = params.get("job_type")
        job_id = params.get("job_id")
        job_params = params.get("params", {})

        if job_type not in self._job_classes:
            raise ValidationError(f"Unknown job type: {job_type}")

        await queue.add_job(self._job_classes[job_type], job_id, job_params)

        return {
            "message": f"Job {job_id} added successfully",
            "job_id": job_id,
            "job_type": job_type,
            "status": "added",
        }


class QueueStartJobCommand(_QueueCommandBase):
    """Command to start a job."""

    def __init__(self, queue_accessor: QueueAccessor) -> None:
        """
        Configure the start-job command metadata.

        Args:
            queue_accessor: Callable that returns the queue system to operate on.
        """
        super().__init__(queue_accessor)
        self.name = "queue_start_job"
        self.description = "Start a job in the queue"
        self.version = "1.0.0"

    def get_schema(self) -> Dict[str, Any]:
        """Describe the schema for the start job command."""
        return {
            "type": "object",
            "properties": {
                "job_id": {"type": "string", "description": "Job identifier to start"}
            },
            "required": ["job_id"],
        }

    async def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute queue start job command."""
        queue = self._get_queue()
        job_id = params.get("job_id")
        await queue.start_job(job_id)
        return {
            "message": f"Job {job_id} started successfully",
            "job_id": job_id,
            "status": "started",
        }


class QueueStopJobCommand(_QueueCommandBase):
    """Command to stop a job."""

    def __init__(self, queue_accessor: QueueAccessor) -> None:
        """
        Configure the stop-job command metadata.

        Args:
            queue_accessor: Callable that returns the queue system to operate on.
        """
        super().__init__(queue_accessor)
        self.name = "queue_stop_job"
        self.description = "Stop a job in the queue"
        self.version = "1.0.0"

    def get_schema(self) -> Dict[str, Any]:
        """Describe the schema for the stop job command."""
        return {
            "type": "object",
            "properties": {
                "job_id": {"type": "string", "description": "Job identifier to stop"}
            },
            "required": ["job_id"],
        }

    async def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute queue stop job command."""
        queue = self._get_queue()
        job_id = params.get("job_id")
        await queue.stop_job(job_id)
        return {
            "message": f"Job {job_id} stopped successfully",
            "job_id": job_id,
            "status": "stopped",
        }


class QueueGetJobStatusCommand(_QueueCommandBase):
    """Command to get job status."""

    def __init__(self, queue_accessor: QueueAccessor) -> None:
        """
        Configure the job-status command metadata.

        Args:
            queue_accessor: Callable providing the active queue system instance.
        """
        super().__init__(queue_accessor)
        self.name = "queue_get_job_status"
        self.description = "Get status of a job"
        self.version = "1.0.0"

    def get_schema(self) -> Dict[str, Any]:
        """Describe the schema for the get job status command."""
        return {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "string",
                    "description": "Job identifier to get status for",
                }
            },
            "required": ["job_id"],
        }

    async def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute queue get job status command."""
        queue = self._get_queue()
        job_id = params.get("job_id")
        status = await queue.get_job_status(job_id)
        return {"job_id": job_id, "status": status}


class QueueListJobsCommand(_QueueCommandBase):
    """Command to list all jobs."""

    def __init__(self, queue_accessor: QueueAccessor) -> None:
        """
        Configure the list-jobs command metadata.

        Args:
            queue_accessor: Callable providing the active queue system instance.
        """
        super().__init__(queue_accessor)
        self.name = "queue_list_jobs"
        self.description = "List all jobs in the queue"
        self.version = "1.0.0"

    def get_schema(self) -> Dict[str, Any]:
        """Describe the schema for the list jobs command."""
        return {"type": "object", "properties": {}}

    async def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute queue list jobs command."""
        queue = self._get_queue()
        jobs = await queue.list_jobs()
        return {"jobs": jobs, "count": len(jobs)}


class QueueHealthCommand(_QueueCommandBase):
    """Command to check queue system health."""

    def __init__(self, queue_accessor: QueueAccessor) -> None:
        """
        Configure the health-check command metadata.

        Args:
            queue_accessor: Callable providing the active queue system instance.
        """
        super().__init__(queue_accessor)
        self.name = "queue_health"
        self.description = "Check queue system health"
        self.version = "1.0.0"

    def get_schema(self) -> Dict[str, Any]:
        """Describe the schema for the health command."""
        return {"type": "object", "properties": {}}

    async def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute queue health command."""
        queue = self._get_queue()
        is_running = queue.is_running()
        return {
            "queue_running": is_running,
            "status": "healthy" if is_running else "unhealthy",
            "timestamp": asyncio.get_event_loop().time(),
        }


__all__ = [
    "QueueAddJobCommand",
    "QueueStartJobCommand",
    "QueueStopJobCommand",
    "QueueGetJobStatusCommand",
    "QueueListJobsCommand",
    "QueueHealthCommand",
    "QueueAccessor",
    "JobClassMap",
]
