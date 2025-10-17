"""
Configuration for ProcManager.

This module contains the configuration classes and settings
for the Linux-specific process manager.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from dataclasses import dataclass


@dataclass
class ProcManagerConfig:
    """Configuration for ProcManager."""

    registry_path: str = "queuemgr_registry.jsonl"
    proc_dir: str = "/tmp/queuemgr"
    shutdown_timeout: float = 30.0
    cleanup_interval: float = 60.0
    max_concurrent_jobs: int = 10
