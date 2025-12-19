"""
Configuration for ProcessManager.

This module contains the configuration class for ProcessManager.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class ProcessManagerConfig:
    """Configuration for ProcessManager and related manager processes."""

    registry_path: str = "queuemgr_registry.jsonl"
    shutdown_timeout: float = 30.0
    cleanup_interval: float = 60.0
    command_timeout: float = 30.0
    max_concurrent_jobs: int = 10
    max_queue_size: Optional[int] = None
    per_job_type_limits: Optional[Dict[str, int]] = None
    completed_job_retention_seconds: Optional[float] = None
