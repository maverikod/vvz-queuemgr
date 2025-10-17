"""
Inter-process communication for Queue Manager.

This module provides IPC functionality for job state management,
commands, and progress tracking using multiprocessing.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

# Import all functionality from separate modules
from .ipc_manager import get_manager, create_job_shared_state
from .ipc_operations import (
    with_job_lock,
    update_job_state,
    read_job_state,
    set_command,
    get_command,
    clear_command,
    set_status,
    set_progress,
    get_progress,
)

# Re-export for backward compatibility
__all__ = [
    "get_manager",
    "create_job_shared_state",
    "with_job_lock",
    "update_job_state",
    "read_job_state",
    "set_command",
    "get_command",
    "clear_command",
    "set_status",
    "set_progress",
    "get_progress",
]
