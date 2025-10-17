"""
Configuration for ProcessManager.

This module contains the configuration class for ProcessManager.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from dataclasses import dataclass


@dataclass
class ProcessManagerConfig:
    """Configuration for ProcessManager."""

    registry_path: str = "queuemgr_registry.jsonl"
    shutdown_timeout: float = 30.0
    cleanup_interval: float = 60.0
    max_concurrent_jobs: int = 10
