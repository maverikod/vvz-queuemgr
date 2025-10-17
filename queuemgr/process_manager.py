"""
Process manager for the queue system.

This module provides a high-level interface for managing the entire queue system
in a separate process with automatic cleanup.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

# Import all functionality from separate modules
from .process_core import ProcessManager
from .process_config import ProcessManagerConfig
from .process_context import ProcessManagerContext

# Re-export for backward compatibility
__all__ = ["ProcessManager", "ProcessManagerConfig", "ProcessManagerContext"]
