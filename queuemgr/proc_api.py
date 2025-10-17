"""
Simple API for the queue system using /proc filesystem.

This module provides a high-level, user-friendly interface for managing
jobs with minimal user intervention using Linux /proc filesystem.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import atexit
import signal
import sys
from typing import Dict, Any, Optional, Type, List
from contextlib import contextmanager

from .proc_manager import ProcManager, ProcManagerConfig
from .jobs.base import QueueJobBase
from .exceptions import ProcessControlError


class ProcQueueSystem:
    """
    Simple API for the queue system using /proc filesystem.

    Provides a high-level interface that automatically manages the process
    manager and handles cleanup using Linux /proc filesystem.
    """

    def __init__(
        self,
        registry_path: str = "queuemgr_registry.jsonl",
        proc_dir: str = "/tmp/queuemgr",
        shutdown_timeout: float = 30.0,
    ):
        """
        Initialize the queue system.

        Args:
            registry_path: Path to the registry file.
            proc_dir: Directory for /proc communication.
            shutdown_timeout: Timeout for graceful shutdown.
        """
        self.config = ProcManagerConfig(
            registry_path=registry_path,
            proc_dir=proc_dir,
            shutdown_timeout=shutdown_timeout,
        )
        self._manager: Optional[ProcManager] = None
        self._is_initialized = False

    def start(self) -> None:
        """
        Start the queue system.

        Raises:
            ProcessControlError: If the system is already running or fails to start.
        """
        if self._is_initialized:
            raise ProcessControlError(
                "queue", "start", Exception("Queue system is already running")
            )

        self._manager = ProcManager(self.config)
        self._manager.start()
        self._is_initialized = True

        # Register cleanup handlers
        atexit.register(self._cleanup)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def stop(self) -> None:
        """Stop the queue system and all running jobs."""
        if not self._is_initialized:
            return

        if self._manager:
            self._manager.stop()
            self._manager = None

        self._is_initialized = False

    def is_running(self) -> bool:
        """Check if the queue system is running."""
        return (
            self._is_initialized
            and self._manager is not None
            and self._manager.is_running()
        )

    def add_job(
        self, job_class: Type[QueueJobBase], job_id: str, params: Dict[str, Any]
    ) -> None:
        """
        Add a job to the queue.

        Args:
            job_class: Job class to instantiate.
            job_id: Unique job identifier.
            params: Job parameters.

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        self._ensure_running()
        self._manager.add_job(job_class, job_id, params)

    def start_job(self, job_id: str) -> None:
        """
        Start a job.

        Args:
            job_id: Job identifier.

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        self._ensure_running()
        self._manager.start_job(job_id)

    def stop_job(self, job_id: str) -> None:
        """
        Stop a job.

        Args:
            job_id: Job identifier.

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        self._ensure_running()
        self._manager.stop_job(job_id)

    def delete_job(self, job_id: str, force: bool = False) -> None:
        """
        Delete a job.

        Args:
            job_id: Job identifier.
            force: Force deletion even if job is running.

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        self._ensure_running()
        self._manager.delete_job(job_id, force)

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get job status.

        Args:
            job_id: Job identifier.

        Returns:
            Job status information.

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        self._ensure_running()
        return self._manager.get_job_status(job_id)

    def list_jobs(self) -> List[Dict[str, Any]]:
        """
        List all jobs.

        Returns:
            List of job information.

        Raises:
            ProcessControlError: If the system is not running or command fails.
        """
        self._ensure_running()
        return self._manager.list_jobs()

    def _ensure_running(self) -> None:
        """Ensure the system is running."""
        if not self.is_running():
            raise ProcessControlError(
                "queue", "operation", Exception("Queue system is not running")
            )

    def _cleanup(self) -> None:
        """Cleanup handler for atexit."""
        self.stop()

    def _signal_handler(self, signum, frame) -> None:
        """Signal handler for graceful shutdown."""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.stop()
        sys.exit(0)


# Global queue system instance
_global_proc_queue: Optional[ProcQueueSystem] = None


def get_proc_queue_system(
    registry_path: str = "queuemgr_registry.jsonl",
    proc_dir: str = "/tmp/queuemgr",
    shutdown_timeout: float = 30.0,
) -> ProcQueueSystem:
    """
    Get or create the global proc queue system instance.

    Args:
        registry_path: Path to the registry file.
        proc_dir: Directory for /proc communication.
        shutdown_timeout: Timeout for graceful shutdown.

    Returns:
        The global proc queue system instance.
    """
    global _global_proc_queue

    if _global_proc_queue is None:
        _global_proc_queue = ProcQueueSystem(registry_path, proc_dir, shutdown_timeout)

    return _global_proc_queue


@contextmanager
def proc_queue_system(
    registry_path: str = "queuemgr_registry.jsonl",
    proc_dir: str = "/tmp/queuemgr",
    shutdown_timeout: float = 30.0,
):
    """
    Context manager for the proc queue system.

    Automatically starts and stops the queue system.

    Args:
        registry_path: Path to the registry file.
        proc_dir: Directory for /proc communication.
        shutdown_timeout: Timeout for graceful shutdown.

    Yields:
        ProcQueueSystem: The queue system instance.
    """
    queue = ProcQueueSystem(registry_path, proc_dir, shutdown_timeout)

    try:
        queue.start()
        yield queue
    finally:
        queue.stop()


# Convenience functions for the global proc queue system
def add_job(job_class: Type[QueueJobBase], job_id: str, params: Dict[str, Any]) -> None:
    """Add a job to the global proc queue system."""
    get_proc_queue_system().add_job(job_class, job_id, params)


def start_job(job_id: str) -> None:
    """Start a job in the global proc queue system."""
    get_proc_queue_system().start_job(job_id)


def stop_job(job_id: str) -> None:
    """Stop a job in the global proc queue system."""
    get_proc_queue_system().stop_job(job_id)


def delete_job(job_id: str, force: bool = False) -> None:
    """Delete a job from the global proc queue system."""
    get_proc_queue_system().delete_job(job_id, force)


def get_job_status(job_id: str) -> Dict[str, Any]:
    """Get job status from the global proc queue system."""
    return get_proc_queue_system().get_job_status(job_id)


def list_jobs() -> List[Dict[str, Any]]:
    """List all jobs in the global proc queue system."""
    return get_proc_queue_system().list_jobs()


def start_proc_queue_system(
    registry_path: str = "queuemgr_registry.jsonl",
    proc_dir: str = "/tmp/queuemgr",
    shutdown_timeout: float = 30.0,
) -> None:
    """Start the global proc queue system."""
    get_proc_queue_system(registry_path, proc_dir, shutdown_timeout).start()


def stop_proc_queue_system() -> None:
    """Stop the global proc queue system."""
    global _global_proc_queue
    if _global_proc_queue:
        _global_proc_queue.stop()
        _global_proc_queue = None
