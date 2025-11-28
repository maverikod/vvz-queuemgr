"""
Core QueueJobBase functionality.

This module contains the core QueueJobBase class that provides
the base functionality for all queue jobs.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import sys
from abc import ABC, abstractmethod
from multiprocessing import Process
from typing import Dict, Any, Optional, Union, List, Type

from queuemgr.core.registry import JsonlRegistry
from queuemgr.core.types import JobId, JobStatus, JobCommand
from queuemgr.core.ipc import (
    get_command,
    update_job_state,
    read_job_state,
    set_command,
)
from queuemgr.exceptions import ValidationError, ProcessControlError
from .log_capture import LogCapture


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

    @staticmethod
    def _job_loop_static(
        job_class: Type["QueueJobBase"],
        job_id: JobId,
        params: Dict[str, Any],
        shared_state: Optional[Dict[str, Any]],
    ) -> None:
        """
        Static wrapper for job loop execution in child process.

        This method is used to properly handle multiprocessing spawn mode,
        where bound methods cannot be pickled correctly. It reconstructs
        the job instance in the child process and executes the job loop.

        Args:
            job_class: The job class to instantiate.
            job_id: Job identifier.
            params: Job parameters.
            shared_state: Shared state dictionary for IPC (optional).

        Raises:
            ProcessControlError: If job instantiation or execution fails.
        """
        try:
            # Create new job instance in child process
            job_instance = job_class(job_id, params)

            # Set shared state (registry is not needed in child process)
            if shared_state is not None:
                job_instance._set_shared_state(shared_state)

            # Execute job loop
            job_instance._job_loop()
        except Exception as exc:  # pylint: disable=broad-except
            # If shared state is available, try to update error status
            if shared_state is not None:
                try:
                    update_job_state(shared_state, status=JobStatus.ERROR)
                except Exception:  # pylint: disable=broad-except
                    pass  # Ignore errors when updating error status
            raise ProcessControlError(
                job_id, "execute", f"Failed to execute job: {exc}"
            ) from exc

    def start_process(self) -> None:
        """
        Start the job process.

        Uses a static wrapper method to ensure compatibility with
        multiprocessing spawn mode, which is required for CUDA compatibility.

        Raises:
            ProcessControlError: If the job is already running or start fails.
        """
        if self.is_running():
            raise ProcessControlError(self.job_id, "start", "Job is already running")

        try:
            # Use static method wrapper for spawn mode compatibility
            # This ensures proper pickling/unpickling in child process
            self._process = Process(
                target=self._job_loop_static,
                args=(
                    self.__class__,
                    self.job_id,
                    self.params,
                    self._shared_state,
                ),
                name=f"Job-{self.job_id}",
                daemon=True,
            )
            self._process.start()
        except Exception as exc:  # pylint: disable=broad-except
            raise ProcessControlError(
                self.job_id, "start", f"Failed to start job: {exc}"
            ) from exc

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
            if self._shared_state is not None:
                set_command(self._shared_state, JobCommand.STOP)

            if self._process:
                self._process.join(timeout=timeout)
                if self._process.is_alive():
                    raise ProcessControlError(
                        self.job_id,
                        "stop",
                        f"Job failed to stop within timeout {timeout}",
                    )
        except ProcessControlError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            raise ProcessControlError(self.job_id, "stop", f"Failed to stop job: {exc}")

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
        except Exception as exc:  # pylint: disable=broad-except
            raise ProcessControlError(
                self.job_id, "terminate", f"Failed to terminate job: {exc}"
            ) from exc

    def _job_loop(self) -> None:
        """Main job execution loop."""
        # Capture stdout/stderr if shared state is available
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        stdout_capture: Optional[LogCapture] = None
        stderr_capture: Optional[LogCapture] = None

        try:
            if self._shared_state is not None:
                # Set up log capture
                stdout_logs = self._shared_state.get("stdout")
                stderr_logs = self._shared_state.get("stderr")
                if stdout_logs is not None:
                    stdout_capture = LogCapture(stdout_logs)
                    sys.stdout = stdout_capture  # type: ignore
                if stderr_logs is not None:
                    stderr_capture = LogCapture(stderr_logs)
                    sys.stderr = stderr_capture  # type: ignore

            # Update status to running
            if self._shared_state is not None:
                update_job_state(self._shared_state, status=JobStatus.RUNNING)

            # Call on_start hook
            self.on_start()

            # Check for STOP or DELETE commands before execute
            if self._shared_state is not None:
                command = get_command(self._shared_state)
                if command == JobCommand.STOP:
                    try:
                        self._handle_stop()
                    except Exception as stop_error:  # pylint: disable=broad-except
                        self._handle_error(stop_error)
                    return
                elif command == JobCommand.DELETE:
                    try:
                        self._handle_delete()
                    except Exception as delete_error:  # pylint: disable=broad-except
                        self._handle_error(delete_error)
                    return

            # Execute the job
            self.execute()

            # Check for STOP or DELETE commands after execute
            if self._shared_state is not None:
                command = get_command(self._shared_state)
                if command == JobCommand.STOP:
                    try:
                        self._handle_stop()
                    except Exception as stop_error:  # pylint: disable=broad-except
                        self._handle_error(stop_error)
                    return
                elif command == JobCommand.DELETE:
                    try:
                        self._handle_delete()
                    except Exception as delete_error:  # pylint: disable=broad-except
                        self._handle_error(delete_error)
                    return

            # Handle completion
            try:
                self._handle_completion()
            except Exception as completion_error:  # pylint: disable=broad-except
                self._handle_error(completion_error)

        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as exc:
            self.error = exc
            try:
                self._handle_error(exc)
            except Exception:  # pylint: disable=broad-except
                # Swallow handler errors to keep shutdown path predictable.
                pass
        finally:
            # Restore original stdout/stderr
            if stdout_capture is not None:
                stdout_capture.flush()
                sys.stdout = original_stdout
            if stderr_capture is not None:
                stderr_capture.flush()
                sys.stderr = original_stderr

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
                self.get_status()
                # Registry persistence is implemented by concrete registries.
            except (
                OSError,
                IOError,
                ValueError,
                TimeoutError,
                ProcessControlError,
            ):
                # If registry write fails, log but don't crash
                pass
