"""
Simplified AsyncIO queue system without separate processes.

This module provides a simplified asyncio-compatible queue system
that works directly in the current process without multiprocessing.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import asyncio
import time
from typing import Dict, Any, Optional, Type, List
from contextlib import asynccontextmanager

from .jobs.base import QueueJobBase
from .core.registry import JsonlRegistry
from queuemgr.exceptions import ProcessControlError


class AsyncSimpleQueue:
    """
    Simplified asyncio-compatible queue system.

    This version works directly in the current process without
    separate processes, making it suitable for asyncio applications.
    """

    def __init__(
        self,
        registry_path: str = None,
        max_concurrent_jobs: int = 5,
    ):
        """
        Initialize the async simple queue.

        Args:
            registry_path: Path to the registry file.
            max_concurrent_jobs: Maximum number of concurrent jobs.
        """
        self.registry_path = registry_path or "queuemgr_registry.jsonl"
        self.max_concurrent_jobs = max_concurrent_jobs
        self._registry: Optional[JsonlRegistry] = None
        self._jobs: Dict[str, QueueJobBase] = {}
        self._running_jobs: Dict[str, asyncio.Task] = {}
        self._is_running = False

    async def start(self) -> None:
        """
        Start the queue system.

        Raises:
            ProcessControlError: If the system is already running or fails to start.
        """
        if self._is_running:
            raise ProcessControlError(
                "system", "start", "Queue system is already running"
            )

        try:
            # Only create registry if path is provided and has directory
            if self.registry_path and "/" in self.registry_path:
                self._registry = JsonlRegistry(self.registry_path)
            self._is_running = True
            print("✅ AsyncSimpleQueue started")
        except Exception as e:
            raise ProcessControlError("system", "start", f"Failed to start system: {e}")

    async def stop(self) -> None:
        """
        Stop the queue system.

        Raises:
            ProcessControlError: If the system is not running or fails to stop.
        """
        if not self._is_running:
            return

        try:
            # Cancel all running jobs
            for job_id, task in self._running_jobs.items():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    print(f"✅ Job {job_id} cancelled")

            self._running_jobs.clear()
            self._jobs.clear()
            self._is_running = False
            print("✅ AsyncSimpleQueue stopped")
        except Exception as e:
            raise ProcessControlError("system", "stop", f"Failed to stop system: {e}")

    def is_running(self) -> bool:
        """
        Check if the queue system is running.

        Returns:
            True if the system is running, False otherwise.
        """
        return self._is_running

    async def add_job(
        self, job_class: Type[QueueJobBase], job_id: str, params: Dict[str, Any]
    ) -> None:
        """
        Add a job to the queue.

        Args:
            job_class: Job class to instantiate.
            job_id: Unique job identifier.
            params: Job parameters.

        Raises:
            ProcessControlError: If the system is not running or job already exists.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "add_job", "Queue system is not running"
            )

        if job_id in self._jobs:
            raise ProcessControlError(
                "system", "add_job", f"Job {job_id} already exists"
            )

        try:
            # Create job instance
            job = job_class(job_id, params)
            self._jobs[job_id] = job

            print(f"✅ Job {job_id} added")
        except Exception as e:
            raise ProcessControlError("system", "add_job", f"Failed to add job: {e}")

    async def start_job(self, job_id: str) -> None:
        """
        Start a job.

        Args:
            job_id: Job identifier.

        Raises:
            ProcessControlError: If the system is not running or job not found.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "start_job", "Queue system is not running"
            )

        if job_id not in self._jobs:
            raise ProcessControlError("system", "start_job", f"Job {job_id} not found")

        if job_id in self._running_jobs and not self._running_jobs[job_id].done():
            raise ProcessControlError(
                "system", "start_job", f"Job {job_id} is already running"
            )

        try:
            # Check concurrent job limit
            if len(self._running_jobs) >= self.max_concurrent_jobs:
                raise ProcessControlError(
                    "system", "start_job", "Maximum concurrent jobs reached"
                )

            # Start job in asyncio task
            job = self._jobs[job_id]
            task = asyncio.create_task(self._run_job_async(job))
            self._running_jobs[job_id] = task

            print(f"✅ Job {job_id} started")
        except Exception as e:
            raise ProcessControlError(
                "system", "start_job", f"Failed to start job: {e}"
            )

    async def stop_job(self, job_id: str) -> None:
        """
        Stop a job.

        Args:
            job_id: Job identifier.

        Raises:
            ProcessControlError: If the system is not running or job not found.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "stop_job", "Queue system is not running"
            )

        if job_id not in self._running_jobs:
            raise ProcessControlError(
                "system", "stop_job", f"Job {job_id} is not running"
            )

        try:
            task = self._running_jobs[job_id]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            del self._running_jobs[job_id]
            print(f"✅ Job {job_id} stopped")
        except Exception as e:
            raise ProcessControlError("system", "stop_job", f"Failed to stop job: {e}")

    async def delete_job(self, job_id: str, force: bool = False) -> None:
        """
        Delete a job.

        Args:
            job_id: Job identifier.
            force: Force deletion even if job is running.

        Raises:
            ProcessControlError: If the system is not running or job not found.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "delete_job", "Queue system is not running"
            )

        if job_id not in self._jobs:
            raise ProcessControlError("system", "delete_job", f"Job {job_id} not found")

        if job_id in self._running_jobs and not force:
            raise ProcessControlError(
                "system", "delete_job", f"Job {job_id} is running, use force=True"
            )

        try:
            # Stop job if running
            if job_id in self._running_jobs:
                await self.stop_job(job_id)

            # Remove job
            del self._jobs[job_id]

            # Job removed from memory

            print(f"✅ Job {job_id} deleted")
        except Exception as e:
            raise ProcessControlError(
                "system", "delete_job", f"Failed to delete job: {e}"
            )

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get job status.

        Args:
            job_id: Job identifier.

        Returns:
            Job status information.

        Raises:
            ProcessControlError: If the system is not running or job not found.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "get_job_status", "Queue system is not running"
            )

        if job_id not in self._jobs:
            raise ProcessControlError(
                "system", "get_job_status", f"Job {job_id} not found"
            )

        try:
            is_running = (
                job_id in self._running_jobs and not self._running_jobs[job_id].done()
            )

            status = {
                "job_id": job_id,
                "status": "running" if is_running else "pending",
                "created_at": time.time(),
                "is_running": is_running,
            }

            if is_running:
                status["started_at"] = time.time()

            return status
        except Exception as e:
            raise ProcessControlError(
                "system", "get_job_status", f"Failed to get job status: {e}"
            )

    async def list_jobs(self) -> List[Dict[str, Any]]:
        """
        List all jobs.

        Returns:
            List of job information.

        Raises:
            ProcessControlError: If the system is not running.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "list_jobs", "Queue system is not running"
            )

        try:
            jobs = []
            for job_id in self._jobs:
                status = await self.get_job_status(job_id)
                jobs.append(status)

            return jobs
        except Exception as e:
            raise ProcessControlError(
                "system", "list_jobs", f"Failed to list jobs: {e}"
            )

    async def _run_job_async(self, job: QueueJobBase) -> None:
        """Run a job asynchronously."""
        try:
            # Run job in executor to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, job.execute)
            print(f"✅ Job {job.job_id} completed successfully")
        except Exception as e:
            print(f"❌ Job {job.job_id} failed: {e}")
        finally:
            # Clean up running job
            if job.job_id in self._running_jobs:
                del self._running_jobs[job.job_id]


@asynccontextmanager
async def async_simple_queue_context(
    registry_path: str = "queuemgr_registry.jsonl",
    max_concurrent_jobs: int = 5,
):
    """
    AsyncIO-compatible context manager for the simple queue system.

    Args:
        registry_path: Path to the registry file.
        max_concurrent_jobs: Maximum number of concurrent jobs.

    Yields:
        AsyncSimpleQueue: The async simple queue instance.

    Example:
        ```python
        async with async_simple_queue_context() as queue:
            await queue.add_job(MyJob, "job1", {"param": "value"})
            await queue.start_job("job1")
            status = await queue.get_job_status("job1")
        ```
    """
    queue_system = AsyncSimpleQueue(registry_path, max_concurrent_jobs)

    try:
        await queue_system.start()
        yield queue_system
    finally:
        await queue_system.stop()
