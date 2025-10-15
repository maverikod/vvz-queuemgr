"""
Inter-process communication primitives for the queue manager system.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

from contextlib import contextmanager
from multiprocessing import Manager
from typing import Any, Dict, Optional

from .types import JobCommand, JobStatus

# TYPE_CHECKING imports removed as they're not needed


def get_manager() -> Any:
    """
    Return a process-shared Manager instance for the queue runtime.

    Returns:
        Manager: A multiprocessing Manager instance for creating shared
        objects.
    """
    return Manager()


def create_job_shared_state(manager: Any) -> Dict[str, Any]:
    """
    Create and return shared variables for a job.

    Creates shared state including status, command, progress, description,
    result, and mutex. All shared variables are thread/process safe and can
    be accessed from multiple processes.

    Args:
        manager: Multiprocessing Manager instance for creating shared objects.

    Returns:
        Dict containing shared state variables:
        - status: Shared Value for job status (JobStatus enum)
        - command: Shared Value for commands (JobCommand enum)
        - progress: Shared Value for progress percentage (0-100)
        - description: Shared string for human-readable description
        - result: Shared object for job result data
        - lock: Lock for synchronizing access to shared state
    """
    shared_state = {
        "status": manager.Value("i", JobStatus.PENDING),
        "command": manager.Value("i", JobCommand.NONE),
        "progress": manager.Value("i", 0),
        "description": manager.Value("c", b""),  # Character array
        "result": manager.Value("O", None),  # Object value
        "lock": manager.Lock(),
    }
    return shared_state


@contextmanager
def with_job_lock(shared_state: Dict[str, Any]):
    """
    Context manager for acquiring the job's mutex for consistent updates/reads.

    Ensures atomic access to multiple shared fields by acquiring the job's lock
    before performing operations and releasing it when done.

    Args:
        shared_state: Dictionary containing shared state variables including
        'lock'.

    Yields:
        The shared_state dictionary for use within the locked context.

    Example:
        with with_job_lock(shared_state) as state:
            state['status'].value = JobStatus.RUNNING
            state['progress'].value = 50
            state['description'].value = b"Processing..."
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        yield shared_state
    finally:
        lock.release()


def update_job_state(
    shared_state: Dict[str, Any],
    status: Optional[JobStatus] = None,
    command: Optional[JobCommand] = None,
    progress: Optional[int] = None,
    description: Optional[str] = None,
    result: Optional[Any] = None,
) -> None:
    """
    Atomically update multiple job state fields.

    Updates the specified fields in the shared state under the job's lock
    to ensure consistency across multiple processes.

    Args:
        shared_state: Dictionary containing shared state variables.
        status: New job status to set (optional).
        command: New command to set (optional).
        progress: New progress percentage to set (optional).
        description: New description to set (optional).
        result: New result to set (optional).

    Raises:
        ValueError: If progress is not between 0 and 100.
    """
    if progress is not None and not (0 <= progress <= 100):
        raise ValueError("Progress must be between 0 and 100")

    # Update each field individually to minimize lock time
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    if status is not None:
        try:
            lock.acquire()
            shared_state["status"].value = status
        finally:
            lock.release()

    if command is not None:
        try:
            lock.acquire()
            shared_state["command"].value = command
        finally:
            lock.release()

    if progress is not None:
        try:
            lock.acquire()
            shared_state["progress"].value = progress
        finally:
            lock.release()

    if description is not None:
        try:
            lock.acquire()
            # Convert string to bytes for character array
            shared_state["description"].value = description.encode("utf-8")
        finally:
            lock.release()

    if result is not None:
        try:
            lock.acquire()
            shared_state["result"].value = result
        finally:
            lock.release()


def read_job_state(shared_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Atomically read all job state fields.

    Reads all fields from the shared state under the job's lock
    to ensure consistency across multiple processes.

    Args:
        shared_state: Dictionary containing shared state variables.

    Returns:
        Dictionary containing current state values:
        - status: Current job status
        - command: Current command
        - progress: Current progress percentage
        - description: Current description (decoded from bytes)
        - result: Current result
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
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
    finally:
        lock.release()


def set_command(shared_state: Dict[str, Any], command: JobCommand) -> None:
    """
    Set a command for the job.

    Args:
        shared_state: Dictionary containing shared state variables.
        command: Command to set.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        shared_state["command"].value = command
    finally:
        lock.release()


def get_command(shared_state: Dict[str, Any]) -> JobCommand:
    """
    Get the current command for the job.

    Args:
        shared_state: Dictionary containing shared state variables.

    Returns:
        Current command.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        return JobCommand(shared_state["command"].value)
    finally:
        lock.release()


def clear_command(shared_state: Dict[str, Any]) -> None:
    """
    Clear the current command (set to NONE).

    Args:
        shared_state: Dictionary containing shared state variables.
    """
    set_command(shared_state, JobCommand.NONE)


def set_status(shared_state: Dict[str, Any], status: JobStatus) -> None:
    """
    Set job status with minimal lock time.

    Args:
        shared_state: Dictionary containing shared state variables.
        status: Status to set.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        shared_state["status"].value = status
    finally:
        lock.release()


def get_status(shared_state: Dict[str, Any]) -> JobStatus:
    """
    Get job status with minimal lock time.

    Args:
        shared_state: Dictionary containing shared state variables.

    Returns:
        Current job status.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        return JobStatus(shared_state["status"].value)
    finally:
        lock.release()


def set_progress(shared_state: Dict[str, Any], progress: int) -> None:
    """
    Set job progress with minimal lock time.

    Args:
        shared_state: Dictionary containing shared state variables.
        progress: Progress percentage (0-100).

    Raises:
        ValueError: If progress is not between 0 and 100.
    """
    if not (0 <= progress <= 100):
        raise ValueError("Progress must be between 0 and 100")

    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        shared_state["progress"].value = progress
    finally:
        lock.release()


def get_progress(shared_state: Dict[str, Any]) -> int:
    """
    Get job progress with minimal lock time.

    Args:
        shared_state: Dictionary containing shared state variables.

    Returns:
        Current progress percentage.
    """
    lock = shared_state.get("lock")
    if lock is None:
        raise ValueError("Shared state must contain a 'lock' key")

    try:
        lock.acquire()
        return shared_state["progress"].value
    finally:
        lock.release()
