"""
IPC operations for job state management.

This module contains the IPC operations for managing job state,
commands, and progress.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from contextlib import contextmanager
from typing import Dict, Any, Generator, Optional, Union, List
from .types import JobCommand, JobStatus


@contextmanager
def with_job_lock(shared_state: Dict[str, Any]) -> Generator[None, None, None]:
    """
    Context manager for acquiring the job's mutex for consistent updates/reads.

    Ensures atomic access to multiple shared fields by acquiring the job's lock
    before performing operations and releasing it when done.

    Args:
        shared_state: Dictionary containing shared state variables.

    Yields:
        None: Context manager yields control to the caller.

    Raises:
        ValueError: If shared_state doesn't contain a 'lock' key.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        yield shared_state
    finally:
        try:
            lock.release()
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, ignore
            pass


def update_job_state(
    shared_state: Dict[str, Any],
    status: JobStatus = None,
    command: JobCommand = None,
    progress: int = None,
    description: str = None,
    result: Optional[Union[str, int, float, bool, Dict[str, Any], List[Any]]] = None,
) -> None:
    """
    Update job state variables atomically.

    Updates multiple shared state variables in a single atomic operation
    to ensure consistency across processes.

    Args:
        shared_state: Dictionary containing shared state variables.
        status: New job status (optional).
        command: New job command (optional).
        progress: New job progress 0-100 (optional).
        description: New job description (optional).
        result: New job result (optional).

    Raises:
        ValueError: If shared_state doesn't contain required keys.
        ValueError: If progress is not between 0 and 100.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    if progress is not None and not (0 <= progress <= 100):
        raise ValueError("Progress must be between 0 and 100")

    try:
        lock.acquire()

        if status is not None:
            try:
                shared_state["status"].value = status.value
            except (BrokenPipeError, ConnectionResetError):
                # Process is shutting down, ignore
                pass

        if command is not None:
            try:
                shared_state["command"].value = command.value
            except (BrokenPipeError, ConnectionResetError):
                # Process is shutting down, ignore
                pass

        if progress is not None:
            try:
                shared_state["progress"].value = progress
            except (BrokenPipeError, ConnectionResetError):
                # Process is shutting down, ignore
                pass

        if description is not None:
            try:
                shared_state["description"].value = description.encode("utf-8")
            except (BrokenPipeError, ConnectionResetError):
                # Process is shutting down, ignore
                pass

        if result is not None:
            try:
                shared_state["result"].value = result
            except (BrokenPipeError, ConnectionResetError):
                # Process is shutting down, ignore
                pass

    finally:
        try:
            lock.release()
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, ignore
            pass


def read_job_state(shared_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Read all job state variables atomically.

    Reads all shared state variables in a single atomic operation
    to ensure consistency across processes.

    Args:
        shared_state: Dictionary containing shared state variables.

    Returns:
        Dictionary containing current job state:
        - status: Current job status
        - command: Current job command
        - progress: Current job progress (0-100)
        - description: Current job description
        - result: Current job result

    Raises:
        ValueError: If shared_state doesn't contain required keys.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        try:
            return {
                "status": JobStatus(shared_state["status"].value),
                "command": JobCommand(shared_state["command"].value),
                "progress": shared_state["progress"].value,
                "description": (
                    shared_state["description"].value.decode("utf-8")
                    if shared_state["description"].value
                    else ""
                ),
                "result": shared_state["result"].value,
            }
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, return default state
            return {
                "status": JobStatus.PENDING,
                "command": JobCommand.NONE,
                "progress": 0,
                "description": "",
                "result": None,
            }
    finally:
        try:
            lock.release()
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, ignore
            pass


def set_command(shared_state: Dict[str, Any], command: JobCommand) -> None:
    """
    Set the job command.

    Args:
        shared_state: Dictionary containing shared state variables.
        command: Command to set.

    Raises:
        ValueError: If shared_state doesn't contain required keys.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        try:
            shared_state["command"].value = command.value
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, ignore
            pass
    finally:
        try:
            lock.release()
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, ignore
            pass


def get_command(shared_state: Dict[str, Any]) -> JobCommand:
    """
    Get the current command for the job.

    Args:
        shared_state: Dictionary containing shared state variables.

    Returns:
        Current job command.

    Raises:
        ValueError: If shared_state doesn't contain required keys.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        try:
            return JobCommand(shared_state["command"].value)
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, return NONE command
            return JobCommand.NONE
    finally:
        try:
            lock.release()
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, ignore
            pass


def clear_command(shared_state: Dict[str, Any]) -> None:
    """
    Clear the job command (set to NONE).

    Args:
        shared_state: Dictionary containing shared state variables.

    Raises:
        ValueError: If shared_state doesn't contain required keys.
    """
    set_command(shared_state, JobCommand.NONE)


def set_status(shared_state: Dict[str, Any], status: JobStatus) -> None:
    """
    Set the job status.

    Args:
        shared_state: Dictionary containing shared state variables.
        status: Status to set.

    Raises:
        ValueError: If shared_state doesn't contain required keys.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        try:
            shared_state["status"].value = status.value
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, ignore
            pass
    finally:
        try:
            lock.release()
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, ignore
            pass


def set_progress(shared_state: Dict[str, Any], progress: int) -> None:
    """
    Set the job progress.

    Args:
        shared_state: Dictionary containing shared state variables.
        progress: Progress value (0-100).

    Raises:
        ValueError: If shared_state doesn't contain required keys.
        ValueError: If progress is not between 0 and 100.
    """
    if not (0 <= progress <= 100):
        raise ValueError("Progress must be between 0 and 100")

    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        try:
            shared_state["progress"].value = progress
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, ignore
            pass
    finally:
        try:
            lock.release()
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, ignore
            pass


def get_progress(shared_state: Dict[str, Any]) -> int:
    """
    Get job progress with minimal lock time.

    Args:
        shared_state: Dictionary containing shared state variables.

    Returns:
        Current job progress (0-100).

    Raises:
        ValueError: If shared_state doesn't contain required keys.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        try:
            return int(shared_state["progress"].value)
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, return default progress
            return 0
    finally:
        try:
            lock.release()
        except (BrokenPipeError, ConnectionResetError):
            # Process is shutting down, ignore
            pass
