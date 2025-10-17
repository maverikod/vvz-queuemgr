"""
Core QueueJobBase functionality.

This module contains the core QueueJobBase class that provides
the base functionality for all queue jobs.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from abc import ABC, abstractmethod
from multiprocessing import Process
from typing import Dict, Any, Optional, Union, List

from queuemgr.core.registry import JsonlRegistry
from queuemgr.core.types import JobId, JobStatus, JobCommand
from queuemgr.core.ipc import (
    get_command,
    update_job_state,
    read_job_state,
    set_command,
)
from queuemgr.exceptions import ValidationError
from queuemgr.core.exceptions import ProcessControlError


class QueueJobBase(ABC):
    """
    Base class for queue jobs. Each instance is executed in a dedicated
    process.

    Responsibilities:
    - React to Start/Stop/Delete commands from shared command variable.
    - Update shared state variables: status, progress, description, result.
    - Write snapshots to registry via the owning queue.
    """

    def __init__(self, job_id: JobId, params: Dict[str, Any]) -> None:
        """
        Initialize the job with ID and parameters.

        Args:
            job_id: Unique identifier for the job.
            params: Job-specific parameters.

        Raises:
            ValidationError: If job_id is invalid or params are malformed.
        """
        if not job_id or not isinstance(job_id, str):
            raise ValidationError("job_id", job_id, "must be a non-empty string")

        self.job_id = job_id
        self.params = params
        self._registry: Optional[JsonlRegistry] = None  # Will be set by the queue
        self._shared_state: Optional[Dict[str, Any]] = None  # Will be set by the queue
        self._process: Optional[Process] = None
        self.error: Optional[Exception] = None

    @abstractmethod
    def execute(self) -> None:
        """
        Execute the job's main work.

        This method should be implemented by subclasses to perform
        the actual job work. It will be called in the job's process.
        """
        raise NotImplementedError

    def on_start(self) -> None:
        """
        Called when the job starts.

        Override this method to perform any initialization
        when the job starts.
        """
        pass

    def on_stop(self) -> None:
        """
        Called when the job is requested to stop.

        Override this method to perform any cleanup
        when the job is requested to stop.
        """
        pass

    def on_end(self) -> None:
        """
        Called when the job ends normally.

        Override this method to perform any finalization
        when the job ends normally.
        """
        pass

    def on_error(self, exc: BaseException) -> None:
        """
        Called when the job encounters an error.

        Args:
            exc: The exception that caused the failure.
        """
        # Default implementation - subclasses should override
        pass

    def set_result(
        self, result: Union[str, int, float, bool, Dict[str, Any], List[Any], None]
    ) -> None:
        """
        Set the job result.

        Args:
            result: The result data to store.
        """
        if self._shared_state is not None:
            update_job_state(self._shared_state, result=result)

    def _set_registry(self, registry: Optional[JsonlRegistry]) -> None:
        """Set the registry for this job (called by the queue)."""
        self._registry = registry

    def _set_shared_state(self, shared_state: Dict[str, Any]) -> None:
        """Set the shared state for this job (called by the queue)."""
        self._shared_state = shared_state

    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the job.

        Returns:
            Dictionary containing job status information.
        """
        if self._shared_state is None:
            return {
                "status": JobStatus.PENDING,
                "command": JobCommand.NONE,
                "progress": 0,
                "description": "",
                "result": None,
            }

        return read_job_state(self._shared_state)

    def is_running(self) -> bool:
        """
        Check if the job process is running.

        Returns:
            True if running, False otherwise.
        """
        return self._process is not None and self._process.is_alive()

    def start_process(self) -> None:
        """
        Start the job process.

        Raises:
            ProcessControlError: If the job is already running or start fails.
        """
        if self.is_running():
            raise ProcessControlError(self.job_id, "start", "Job is already running")

        try:
            self._process = Process(
                target=self._job_loop, name=f"Job-{self.job_id}", daemon=True
            )
            self._process.start()
        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            raise ProcessControlError(self.job_id, "start", f"Failed to start job: {e}")

    def stop_process(self, timeout: Optional[float] = None) -> None:
        """
        Stop the job process gracefully.

        Args:
            timeout: Maximum time to wait for graceful stop.

        Raises:
            ProcessControlError: If the job is not running or stop fails.
        """
        if not self.is_running():
            return  # Not running, nothing to stop

        try:
            # Send stop command
            if self._shared_state is not None:
                set_command(self._shared_state, JobCommand.STOP)

            # Wait for process to finish
            if self._process:
                self._process.join(timeout=timeout)
        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            raise ProcessControlError(self.job_id, "stop", f"Failed to stop job: {e}")

    def terminate_process(self, force: bool = False) -> None:
        """
        Terminate the job process forcefully.

        Args:
            force: If True, use SIGKILL instead of SIGTERM.

        Raises:
            ProcessControlError: If the job is not running or termination fails.
        """
        if not self.is_running():
            return  # Not running, nothing to terminate

        try:
            if self._process:
                if force:
                    self._process.kill()
                else:
                    self._process.terminate()
        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            raise ProcessControlError(
                self.job_id, "terminate", f"Failed to terminate job: {e}"
            )

    def _job_loop(self) -> None:
        """Main job execution loop."""
        try:
            # Update status to running
            if self._shared_state is not None:
                update_job_state(self._shared_state, status=JobStatus.RUNNING)

            # Call on_start hook
            self.on_start()

            # Check for STOP or DELETE commands before execute
            if self._shared_state is not None:
                command = get_command(self._shared_state)
                if command == JobCommand.STOP:
                    self._handle_stop()
                    return
                elif command == JobCommand.DELETE:
                    self._handle_delete()
                    return

            # Execute the job
            self.execute()

            # Check for STOP or DELETE commands after execute
            if self._shared_state is not None:
                command = get_command(self._shared_state)
                if command == JobCommand.STOP:
                    self._handle_stop()
                    return
                elif command == JobCommand.DELETE:
                    self._handle_delete()
                    return

            # Handle completion
            self._handle_completion()

        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            self._handle_error(e)

    def _handle_stop(self) -> None:
        """Handle stop command."""
        try:
            self.on_stop()
            if self._shared_state is not None:
                update_job_state(self._shared_state, status=JobStatus.STOPPED)
        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            self._handle_error(e)

    def _handle_delete(self) -> None:
        """Handle delete command."""
        try:
            self.on_stop()
            if self._shared_state is not None:
                update_job_state(self._shared_state, status=JobStatus.DELETED)
        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            self._handle_error(e)

    def _handle_completion(self) -> None:
        """Handle job completion."""
        try:
            self.on_end()
            if self._shared_state is not None:
                update_job_state(self._shared_state, status=JobStatus.COMPLETED)
        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            self._handle_error(e)

    def _handle_error(self, exc: Exception) -> None:
        """Handle job error."""
        try:
            self.error = exc
            self.on_error(exc)
            if self._shared_state is not None:
                update_job_state(self._shared_state, status=JobStatus.ERROR)
        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            # If error handling fails, just set the error
            self.error = e

    def _write_to_registry(self) -> None:
        """Write job state to registry."""
        if self._registry is not None:
            try:
                status_data = self.get_status()
                # Write to registry (implementation depends on registry type)
                pass
            except (
                OSError,
                IOError,
                ValueError,
                TimeoutError,
                ProcessControlError,
            ) as e:
                # If registry write fails, log but don't crash
                pass
