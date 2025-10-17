"""
Tests for JobQueue - main test file.

This file imports and runs all JobQueue tests from separate modules.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

# Import all test classes from separate modules
from .test_job_queue_basic import TestJobQueueBasic
from .test_job_queue_operations import TestJobQueueOperations

# Re-export for pytest discovery
__all__ = [
    'TestJobQueueBasic',
    'TestJobQueueOperations'
]