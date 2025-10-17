"""
Context manager for ProcessManager.

This module contains the context manager for ProcessManager.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from typing import Optional
from .process_core import ProcessManager
from .process_config import ProcessManagerConfig


class ProcessManagerContext:
    """
    Context manager for ProcessManager.

    Provides automatic startup and shutdown of the process manager.
    """

    def __init__(self, config: Optional[ProcessManagerConfig] = None):
        """
        Initialize the context manager.

        Args:
            config: Configuration for the process manager.
        """
        self.manager = ProcessManager(config)

    def __enter__(self) -> ProcessManager:
        """Start the manager and return it."""
        self.manager.start()
        return self.manager

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Stop the manager on exit."""
        self.manager.stop()
