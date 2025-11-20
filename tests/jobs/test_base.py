"""
Tests for QueueJobBase - main test file.

This file imports and runs all QueueJobBase tests from separate modules.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

# Import all test classes from separate modules
from tests.jobs.test_base_initialization import TestQueueJobBaseInitialization
from tests.jobs.test_base_process_control import TestQueueJobBaseProcessControl
from tests.jobs.test_base_job_loop import TestQueueJobBaseJobLoop

# Re-export for pytest discovery
__all__ = [
    "TestQueueJobBaseInitialization",
    "TestQueueJobBaseProcessControl",
    "TestQueueJobBaseJobLoop",
]
