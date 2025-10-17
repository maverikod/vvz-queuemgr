"""
IPC manager for creating and managing shared state.

This module contains the manager creation and shared state
management functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from multiprocessing import Manager
from typing import Dict, Any


def get_manager() -> Dict[str, Any]:
    """
    Return a process-shared Manager instance for the queue runtime.

    Returns:
        Manager: A multiprocessing Manager instance for creating shared
        objects.
    """
    return Manager()


def create_job_shared_state(manager: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create and return shared variables for a job.

    Creates shared state including status, command, progress, description,
    result, and mutex. All shared variables are thread/process safe and can
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
        - lock: Shared mutex for thread safety
    """
    shared_state = {
        "status": manager.Value("i", 0),  # JobStatus enum value
        "command": manager.Value("i", 0),  # JobCommand enum value
        "progress": manager.Value("i", 0),  # 0-100
        "description": manager.Value("c", b""),  # UTF-8 encoded string
        "result": manager.Value("O", None),  # Any Python object
        "lock": manager.Lock(),  # Mutex for thread safety
    }
    return shared_state
