"""
Base class for queue jobs.

This module provides the base QueueJobBase class that all
queue jobs must inherit from.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

# Import all functionality from separate modules
from .base_core import QueueJobBase

# Re-export for backward compatibility
__all__ = ["QueueJobBase"]
