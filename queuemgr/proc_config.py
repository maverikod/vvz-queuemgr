"""
Configuration for ProcManager.

This module contains the configuration classes and settings
for the Linux-specific process manager.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class ProcManagerConfig:
    """Configuration for ProcManager."""

    registry_path: str = "queuemgr_registry.jsonl"
    proc_dir: str = "/tmp/queuemgr"
    shutdown_timeout: float = 30.0
    cleanup_interval: float = 60.0
    command_timeout: float = 30.0
    max_concurrent_jobs: int = 10
    max_queue_size: Optional[int] = None
    per_job_type_limits: Optional[Dict[str, int]] = None
    completed_job_retention_seconds: Optional[float] = None
