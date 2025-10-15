"""
Base class for queue jobs that run in separate processes.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from multiprocessing import Process
from typing import Any, Dict, Optional

from ..core.types import JobCommand, JobId, JobStatus
from ..core.ipc import (
    get_command,
    read_job_state,
    set_command,
    update_job_state,
)
from ..exceptions import ProcessControlError


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
            from ..exceptions import ValidationError

            raise ValidationError(
                "job_id", job_id, "must be a non-empty string"
            )

        self.job_id = job_id
        self.params = params
        self._process: Optional[Process] = None
        self._shared_state: Optional[Dict[str, Any]] = None
        self._registry = None  # Will be set by the queue
        self._running = False

    @abstractmethod
    def execute(self) -> None:
        """
        Main job logic. Runs inside the child process.

        This method should implement the actual work to be performed.
        It should periodically check for commands and update progress.
        """
        raise NotImplementedError

    @abstractmethod
    def on_start(self) -> None:
        """
        Hook invoked when the job transitions to RUNNING.

        Use this to perform initialization tasks like allocating resources.
        """
        raise NotImplementedError

    @abstractmethod
    def on_stop(self) -> None:
        """
        Hook invoked on STOP command for graceful interruption.

        Use this to perform cleanup tasks like flushing partial work.
        """
        raise NotImplementedError

    @abstractmethod
    def on_end(self) -> None:
        """
        Hook invoked when the job completes successfully (COMPLETED).

        Use this to perform finalization tasks.
        """
        raise NotImplementedError

    @abstractmethod
    def on_error(self, exc: BaseException) -> None:
        """
        Hook invoked when the job fails (ERROR).

        Args:
            exc: The exception that caused the failure.
        """
        raise NotImplementedError

    def _set_registry(self, registry) -> None:
        """Set the registry for this job (called by the queue)."""
        self._registry = registry

    def _set_shared_state(self, shared_state: Dict[str, Any]) -> None:
        """Set the shared state for this job (called by the queue)."""
        self._shared_state = shared_state

    def _job_loop(self) -> None:
        """
        Main job execution loop that runs in the child process.

        Handles command processing, state updates, and error handling.
        """
        try:
            # Wait for START command
            while True:
                if self._shared_state is None:
                    return
                command = get_command(self._shared_state)
                if command == JobCommand.START:
                    break
                elif command == JobCommand.DELETE:
                    return
                time.sleep(0.1)  # Small delay to prevent busy waiting

            # Set status to RUNNING
            if self._shared_state is not None:
                update_job_state(
                    self._shared_state,
                    status=JobStatus.RUNNING,
                    description="Job started",
                )
            self._write_to_registry()

            # Call on_start hook
            self.on_start()

            # Execute job logic once
            self._running = True
            try:
                self.execute()
                # Job completed successfully
                self._handle_completion()
            except Exception as e:
                self._handle_error(e)

        except Exception as e:
            self._handle_error(e)

    def _handle_stop(self) -> None:
        """Handle STOP command."""
        try:
            self.on_stop()
            self._running = False  # Stop the main execution loop
            if self._shared_state is not None:
                update_job_state(
                    self._shared_state,
                    status=JobStatus.INTERRUPTED,
                    description="Job stopped by user",
                )
            self._write_to_registry()
        except Exception as e:
            self._running = False  # Stop the main execution loop even on error
            if self._shared_state is not None:
                update_job_state(
                    self._shared_state,
                    status=JobStatus.ERROR,
                    description=f"Error during stop: {e}",
                )
            self._write_to_registry()

    def _handle_delete(self) -> None:
        """Handle DELETE command."""
        try:
            self.on_stop()  # Try to stop gracefully first
        except Exception:
            pass  # Ignore errors during delete

        self._running = False  # Stop the main execution loop
        if self._shared_state is not None:
            update_job_state(
                self._shared_state,
                status=JobStatus.INTERRUPTED,
                description="Job deleted",
            )
        self._write_to_registry()

    def _handle_completion(self) -> None:
        """Handle successful job completion."""
        try:
            self.on_end()
            if self._shared_state is not None:
                update_job_state(
                    self._shared_state,
                    status=JobStatus.COMPLETED,
                    progress=100,
                    description="Job completed successfully",
                )
            self._write_to_registry()
        except Exception as e:
            self._handle_error(e)

    def _handle_error(self, exc: BaseException) -> None:
        """Handle job execution error."""
        try:
            self.on_error(exc)
        except Exception:
            pass  # Ignore errors in error handler

        if self._shared_state is not None:
            update_job_state(
                self._shared_state,
                status=JobStatus.ERROR,
                description=f"Job failed: {exc}",
            )
        self._write_to_registry()

    def _write_to_registry(self) -> None:
        """Write current state to registry."""
        if self._registry and self._shared_state:
            try:
                state = read_job_state(self._shared_state)
                from ..core.types import JobRecord
                from datetime import datetime

                record = JobRecord(
                    job_id=self.job_id,
                    status=state["status"],
                    progress=state["progress"],
                    description=state["description"],
                    result=state["result"],
                    created_at=datetime.now(),  # Set by queue
                    updated_at=datetime.now(),
                )
                self._registry.append(record)
            except Exception:
                pass  # Ignore registry write errors

    def start_process(self) -> Process:
        """
        Spawn and start the child process running this job's execute loop.

        Returns:
            Process: The spawned process.

        Raises:
            ProcessControlError: If process creation fails.
        """
        if self._process is not None and self._process.is_alive():
            raise ProcessControlError(
                self.job_id, "start", Exception("Process already running")
            )

        try:
            self._process = Process(
                target=self._job_loop, name=f"Job-{self.job_id}"
            )
            self._process.start()
            return self._process
        except Exception as e:
            raise ProcessControlError(self.job_id, "start", e)

    def stop_process(self, timeout: Optional[float] = None) -> None:
        """
        Request STOP and wait for graceful termination.

        Args:
            timeout: Maximum time to wait for graceful stop (seconds).

        Raises:
            ProcessControlError: If stop fails.
        """
        if self._process is None or not self._process.is_alive():
            return  # Process not running

        try:
            # Send STOP command
            if self._shared_state is not None:
                set_command(self._shared_state, JobCommand.STOP)

            # Wait for process to terminate
            if timeout is not None:
                self._process.join(timeout=timeout)
                if self._process.is_alive():
                    raise ProcessControlError(
                        self.job_id,
                        "stop",
                        Exception(
                            f"Process did not stop within {timeout} seconds"
                        ),
                    )
            else:
                self._process.join()

        except Exception as e:
            raise ProcessControlError(self.job_id, "stop", e)

    def terminate_process(self) -> None:
        """
        Forcefully terminate the child process (used by DELETE).

        Raises:
            ProcessControlError: If termination fails.
        """
        if self._process is None or not self._process.is_alive():
            return  # Process not running

        try:
            # Send DELETE command
            if self._shared_state is not None:
                set_command(self._shared_state, JobCommand.DELETE)

            # Give it a moment to respond to the command
            time.sleep(0.1)

            # Force terminate if still alive
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=5.0)  # Wait up to 5 seconds

                # Force kill if still alive
                if self._process.is_alive():
                    self._process.kill()
                    self._process.join()

        except Exception as e:
            raise ProcessControlError(self.job_id, "terminate", e)

    def is_running(self) -> bool:
        """
        Check if the job process is currently running.

        Returns:
            True if the process is alive, False otherwise.
        """
        return self._process is not None and self._process.is_alive()

    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the job.

        Returns:
            Dictionary containing current job state.
        """
        if self._shared_state is None:
            return {
                "status": JobStatus.PENDING,
                "command": JobCommand.NONE,
                "progress": 0,
                "description": "Job not started",
                "result": None,
            }

        return read_job_state(self._shared_state)
