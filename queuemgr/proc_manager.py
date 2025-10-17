"""
Process manager using /proc filesystem for Linux.

This module provides a high-level interface for managing the entire queue system
using Linux /proc filesystem for inter-process communication.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

# Import all functionality from separate modules
from .proc_config import ProcManagerConfig
from .proc_manager_core import ProcManager

# Re-export for backward compatibility
__all__ = ["ProcManager", "ProcManagerConfig"]
