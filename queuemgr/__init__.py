"""
Queue Manager - Full-featured job queue system with multiprocessing support for Linux.

A production-ready job queue system with automatic process management,
real-time monitoring, systemd integration, and multiple interfaces (CLI, web).

Supports both synchronous and asyncio-based applications.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

__version__ = "1.0.13"
__author__ = "Vasiliy Zdanovskiy"
__email__ = "vasilyvz@gmail.com"

# Import core components
from .jobs.base import QueueJobBase
from .exceptions import (
    QueueManagerError,
    JobNotFoundError,
    JobAlreadyExistsError,
    InvalidJobStateError,
    JobExecutionError,
    RegistryError,
    ProcessControlError,
    ValidationError,
    TimeoutError,
)

# Import synchronous API
from .simple_api import QueueSystem

# Import asyncio API
from .async_simple_api import (
    AsyncQueueSystem,
    async_queue_system_context,
    get_global_async_queue,
    shutdown_global_async_queue,
)

# Import simplified asyncio API
from .async_simple_queue import (
    AsyncSimpleQueue,
    async_simple_queue_context,
)

# Import process managers
from .process_manager import ProcessManager
from .async_process_manager import AsyncProcessManager

# Import queue components
from .queue.job_queue import JobQueue

# Export main components
__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__email__",
    # Core components
    "QueueJobBase",
    # Exceptions
    "QueueManagerError",
    "JobNotFoundError",
    "JobAlreadyExistsError",
    "InvalidJobStateError",
    "JobExecutionError",
    "RegistryError",
    "ProcessControlError",
    "ValidationError",
    "TimeoutError",
    # Synchronous API
    "QueueSystem",
    "ProcessManager",
    "JobQueue",
    # AsyncIO API
    "AsyncQueueSystem",
    "AsyncProcessManager",
    "async_queue_system_context",
    "get_global_async_queue",
    "shutdown_global_async_queue",
    # Simplified AsyncIO API
    "AsyncSimpleQueue",
    "async_simple_queue_context",
]
