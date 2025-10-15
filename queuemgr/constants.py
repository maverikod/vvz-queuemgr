"""
Global constants and configuration keys for the queue manager system.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from typing import Final

# Default timeout values (in seconds)
DEFAULT_STOP_TIMEOUT: Final[float] = 10.0
DEFAULT_SHUTDOWN_TIMEOUT: Final[float] = 30.0
DEFAULT_TERMINATE_TIMEOUT: Final[float] = 5.0

# Progress limits
MIN_PROGRESS: Final[int] = 0
MAX_PROGRESS: Final[int] = 100

# Registry configuration
DEFAULT_REGISTRY_PATH: Final[str] = "runtime/registry.jsonl"
REGISTRY_ENCODING: Final[str] = "utf-8"

# Process configuration
DEFAULT_PROCESS_NAME_PREFIX: Final[str] = "Job-"
DEFAULT_JOB_LOOP_DELAY: Final[float] = 0.1

# File system configuration
DEFAULT_RUNTIME_DIR: Final[str] = "runtime"
DEFAULT_EXAMPLES_DIR: Final[str] = "examples"

# Error messages
ERROR_JOB_NOT_FOUND: Final[str] = "Job with ID '{job_id}' not found"
ERROR_JOB_ALREADY_EXISTS: Final[str] = "Job with ID '{job_id}' already exists"
ERROR_INVALID_JOB_STATE: Final[str] = (
    "Cannot {operation} job '{job_id}' in state '{state}'"
)
ERROR_JOB_EXECUTION_FAILED: Final[str] = "Job '{job_id}' execution failed"
ERROR_REGISTRY_OPERATION_FAILED: Final[str] = "Registry operation failed: {message}"
ERROR_PROCESS_CONTROL_FAILED: Final[str] = (
    "Process control error for job '{job_id}' during {operation}"
)
ERROR_VALIDATION_FAILED: Final[str] = "Validation error for field '{field}': {reason}"
ERROR_OPERATION_TIMEOUT: Final[str] = (
    "Operation '{operation}' timed out after {timeout} seconds"
)

# Status descriptions
DESCRIPTION_JOB_CREATED: Final[str] = "Job created"
DESCRIPTION_JOB_STARTED: Final[str] = "Job started"
DESCRIPTION_JOB_STOPPED: Final[str] = "Job stopped by user"
DESCRIPTION_JOB_DELETED: Final[str] = "Job deleted"
DESCRIPTION_JOB_COMPLETED: Final[str] = "Job completed successfully"
DESCRIPTION_JOB_FAILED: Final[str] = "Job failed: {error}"
DESCRIPTION_JOB_INTERRUPTED: Final[str] = "Job interrupted"

# Configuration keys
CONFIG_REGISTRY_PATH: Final[str] = "registry_path"
CONFIG_MAX_CONCURRENT_JOBS: Final[str] = "max_concurrent_jobs"
CONFIG_DEFAULT_TIMEOUT: Final[str] = "default_timeout"
CONFIG_LOG_LEVEL: Final[str] = "log_level"
CONFIG_LOG_FORMAT: Final[str] = "log_format"

# Logging configuration
DEFAULT_LOG_LEVEL: Final[str] = "INFO"
DEFAULT_LOG_FORMAT: Final[str] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Version information
VERSION: Final[str] = "0.1.0"
AUTHOR: Final[str] = "Vasiliy Zdanovskiy"
EMAIL: Final[str] = "vasilyvz@gmail.com"
