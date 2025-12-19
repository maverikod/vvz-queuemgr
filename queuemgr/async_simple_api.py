"""
AsyncIO-compatible Simple API for the queue system.

This module provides asyncio-compatible versions of the simple API
that work correctly in asyncio applications and web servers.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import asyncio
import logging
import signal
from typing import Dict, Any, Optional, Type, List
from contextlib import asynccontextmanager

from .async_process_manager import AsyncProcessManager
from .jobs.base import QueueJobBase
from queuemgr.exceptions import ProcessControlError


logger = logging.getLogger("queuemgr.async_simple_api")


class AsyncQueueSystem:
    """
    AsyncIO-compatible simple API for the queue system.

    Provides a high-level interface that automatically manages the process
    manager and handles cleanup, designed to work with asyncio applications.
    """

    def __init__(
        self,
        registry_path: str = "queuemgr_registry.jsonl",
        shutdown_timeout: float = 30.0,
        max_queue_size: Optional[int] = None,
        per_job_type_limits: Optional[Dict[str, int]] = None,
        completed_job_retention_seconds: Optional[float] = None,
        command_timeout: float = 30.0,
    ):
        """
        Initialize the async queue system.

        Args:
            registry_path: Path to the registry file.
            shutdown_timeout: Timeout for graceful shutdown.
            max_queue_size: Global maximum number of jobs (optional).
            per_job_type_limits: Dict mapping job_type to max count (optional).
            completed_job_retention_seconds: How long to keep completed/error jobs
                before auto-removal (optional). If None, completed jobs are preserved.
            command_timeout: Maximum time to wait for a manager control command
                response. This timeout applies only to IPC control operations and
                does not limit job execution time.
        """
        self.registry_path = registry_path
        self.shutdown_timeout = shutdown_timeout
        self.max_queue_size = max_queue_size
        self.per_job_type_limits = per_job_type_limits
        self.completed_job_retention_seconds = completed_job_retention_seconds
        self.command_timeout = command_timeout
        self._manager: Optional[AsyncProcessManager] = None
        self._is_initialized = False

    async def start(self) -> None:
        """
        Start the async queue system.

        Raises:
            ProcessControlError: If the system is already running or fails to start.
        """
        if self._is_initialized:
            raise ProcessControlError(
                "system", "start", "Queue system is already running"
            )

        try:
            from .process_config import ProcessManagerConfig

            config = ProcessManagerConfig(
                registry_path=self.registry_path,
                shutdown_timeout=self.shutdown_timeout,
                max_queue_size=self.max_queue_size,
                per_job_type_limits=self.per_job_type_limits,
                completed_job_retention_seconds=self.completed_job_retention_seconds,
                command_timeout=self.command_timeout,
            )
            self._manager = AsyncProcessManager(config)
            await self._manager.start()
            self._is_initialized = True
        except Exception as e:
            raise ProcessControlError("system", "start", f"Failed to start system: {e}")

    async def stop(self) -> None:
        """
        Stop the async queue system.

        Raises:
            ProcessControlError: If the system is not running or fails to stop.
        """
        if not self._is_initialized:
            return

        try:
            if self._manager:
                await self._manager.stop()
            self._is_initialized = False
        except Exception as e:
            raise ProcessControlError("system", "stop", f"Failed to stop system: {e}")

    def is_running(self) -> bool:
        """
        Check if the queue system is running.

        Returns:
            True if the system is running, False otherwise.
        """
        if not self._is_initialized:
            return False
        if self._manager is None:
            return False
        return self._manager.is_running()

    async def add_job(
        self,
        job_class: Type[QueueJobBase],
        job_id: str,
        params: Dict[str, Any],
        timeout: Optional[float] = None,
    ) -> None:
        """
        Add a job to the queue.

        Args:
            job_class: Job class to instantiate.
            job_id: Unique job identifier.
            params: Job parameters.
            timeout: Optional override for the control-plane timeout (seconds).

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "add_job", "Queue system is not running"
            )

        await self._manager.add_job(job_class, job_id, params, timeout=timeout)

    async def start_job(self, job_id: str, timeout: Optional[float] = None) -> None:
        """
        Start a job.

        Args:
            job_id: Job identifier.
            timeout: Optional override for the control-plane timeout (seconds).

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "start_job", "Queue system is not running"
            )

        await self._manager.start_job(job_id, timeout=timeout)

    async def start_job_background(
        self, job_id: str, timeout: Optional[float] = None
    ) -> None:
        """
        Start a job in the background without blocking the caller.

        This helper schedules an internal task that calls ``start_job`` and
        immediately returns control to the caller. Any control-plane errors are
        logged but not propagated.

        Args:
            job_id: Job identifier.
            timeout: Optional override for the control-plane timeout (seconds).

        Raises:
            ProcessControlError: If the queue system is not running.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "start_job_background", "Queue system is not running"
            )

        async def _background_start() -> None:
            """Start job and log, but do not propagate, control errors."""
            if self._manager is None:
                return

            try:
                await self._manager.start_job(job_id, timeout=timeout)
            except ProcessControlError as exc:
                logger.error(
                    "Background start_job for job '%s' failed: %s",
                    job_id,
                    exc,
                )

        asyncio.create_task(_background_start())

    async def stop_job(self, job_id: str, timeout: Optional[float] = None) -> None:
        """
        Stop a job.

        Args:
            job_id: Job identifier.
            timeout: Optional override for the control-plane timeout (seconds).

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "stop_job", "Queue system is not running"
            )

        await self._manager.stop_job(job_id, timeout=timeout)

    async def delete_job(self, job_id: str, force: bool = False) -> None:
        """
        Delete a job.

        Args:
            job_id: Job identifier.
            force: Force deletion even if job is running.

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "delete_job", "Queue system is not running"
            )

        await self._manager.delete_job(job_id, force)

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get job status.

        Args:
            job_id: Job identifier.

        Returns:
            Job status information.

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "get_job_status", "Queue system is not running"
            )

        return await self._manager.get_job_status(job_id)

    async def list_jobs(self) -> List[Dict[str, Any]]:
        """
        List all jobs.

        Returns:
            List of job information.

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "list_jobs", "Queue system is not running"
            )

        return await self._manager.list_jobs()

    async def get_job_logs(self, job_id: str) -> Dict[str, List[str]]:
        """
        Get stdout and stderr logs for a job.

        Args:
            job_id: Job identifier.

        Returns:
            Dictionary containing:
            - stdout: List of stdout log lines
            - stderr: List of stderr log lines

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError(
                "system", "get_job_logs", "Queue system is not running"
            )

        return await self._manager.get_job_logs(job_id)


@asynccontextmanager
async def async_queue_system_context(
    registry_path: str = "queuemgr_registry.jsonl",
    shutdown_timeout: float = 30.0,
    max_queue_size: Optional[int] = None,
    per_job_type_limits: Optional[Dict[str, int]] = None,
):
    """
    AsyncIO-compatible context manager for the queue system.

    Args:
        registry_path: Path to the registry file.
        shutdown_timeout: Timeout for graceful shutdown.
        max_queue_size: Global maximum number of jobs (optional).
        per_job_type_limits: Dict mapping job_type to max count (optional).

    Yields:
        AsyncQueueSystem: The async queue system instance.

    Example:
        ```python
        async with async_queue_system_context() as queue:
            await queue.add_job(MyJob, "job1", {"param": "value"})
            await queue.start_job("job1")
            status = await queue.get_job_status("job1")
        ```
    """
    queue_system = AsyncQueueSystem(
        registry_path,
        shutdown_timeout,
        max_queue_size=max_queue_size,
        per_job_type_limits=per_job_type_limits,
    )

    try:
        await queue_system.start()
        yield queue_system
    finally:
        await queue_system.stop()


# Global async queue system instance for convenience
_global_async_queue: Optional[AsyncQueueSystem] = None


async def get_global_async_queue() -> AsyncQueueSystem:
    """
    Get the global async queue system instance.

    Returns:
        The global async queue system instance.

    Raises:
        ProcessControlError: If the global queue system is not initialized.
    """
    global _global_async_queue

    if _global_async_queue is None:
        _global_async_queue = AsyncQueueSystem()
        await _global_async_queue.start()

    return _global_async_queue


async def shutdown_global_async_queue() -> None:
    """
    Shutdown the global async queue system.

    This function should be called during application shutdown.
    """
    global _global_async_queue

    if _global_async_queue is not None:
        await _global_async_queue.stop()
        _global_async_queue = None


# Register cleanup function
async def _cleanup_handler():
    """Cleanup handler for graceful shutdown."""
    await shutdown_global_async_queue()


# Register cleanup for different signal types
def _setup_async_cleanup():
    """Setup async cleanup handlers."""
    try:
        # Only register if we're in the main thread
        if hasattr(signal, "SIGTERM"):
            signal.signal(
                signal.SIGTERM, lambda s, f: asyncio.create_task(_cleanup_handler())
            )
        if hasattr(signal, "SIGINT"):
            signal.signal(
                signal.SIGINT, lambda s, f: asyncio.create_task(_cleanup_handler())
            )
    except ValueError:
        # Signals can only be registered in the main thread
        # This is expected in some contexts, so we ignore the error
        pass


# Setup cleanup on module import
_setup_async_cleanup()
