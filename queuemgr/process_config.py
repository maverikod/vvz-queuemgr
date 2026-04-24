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
    # Control-plane timeout: max time to wait for a manager command response (IPC only).
    command_timeout: float = 30.0
    # Max time the manager waits for a job to stop when processing stop_job
    # (avoids blocking the control loop).
    stop_job_wait_timeout: float = 10.0
    max_concurrent_jobs: int = 10
    max_queue_size: Optional[int] = None
    per_job_type_limits: Optional[Dict[str, int]] = None
    completed_job_retention_seconds: Optional[float] = None
    terminal_job_retention_seconds: Optional[float] = None
    failed_terminal_retention_seconds: Optional[float] = None
    stopped_terminal_retention_seconds: Optional[float] = None
    deleted_terminal_retention_seconds: Optional[float] = None
    max_retained_terminal_jobs: int = 1000
