"""
IPC manager for creating and managing shared state.

This module contains the manager creation and shared state
management functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from typing import Dict, Any

from queuemgr.mp_context import get_mp_context


def get_manager() -> Dict[str, Any]:
    """
    Return a process-shared Manager instance for the queue runtime.

    The manager is created from the queuemgr-local multiprocessing context
    (see queuemgr.mp_context.get_mp_context) rather than the default
    ``multiprocessing.Manager()``. The default Manager() call implicitly
    spawns its server process using the process-wide default start method;
    routing it through the local context keeps it consistent with the
    Process/Queue/Event objects used elsewhere in the package and avoids an
    unexpected fork of a host application (e.g. a live asyncio server).

    Returns:
        Manager: A multiprocessing SyncManager instance for creating shared
        objects.
    """
    return get_mp_context().Manager()


def create_job_shared_state(manager: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create and return shared variables for a job.

    Creates shared state including status, command, progress, description,
    result, logs, and mutex. All shared variables are thread/process safe and can
    be accessed from multiple processes.

    Args:
        manager: Multiprocessing Manager instance.

    Returns:
        Dict containing shared state variables:
        - status: Shared integer for job status
        - command: Shared integer for job command
        - progress: Shared integer for job progress (0-100)
        - description: Shared string for job description
        - result: Shared value for job result
        - stdout: Shared list for stdout output lines
        - stderr: Shared list for stderr output lines
        - lock: Shared mutex for thread safety
    """
    shared_state = {
        "status": manager.Value("i", 0),  # JobStatus enum value
        "command": manager.Value("i", 0),  # JobCommand enum value
        "progress": manager.Value("i", 0),  # 0-100
        "description": manager.Value("c", b""),  # UTF-8 encoded string
        "result": manager.Value("O", None),  # Any Python object
        "stdout": manager.list(),  # List of stdout lines
        "stderr": manager.list(),  # List of stderr lines
        "lock": manager.Lock(),  # Mutex for thread safety
    }
    return shared_state
