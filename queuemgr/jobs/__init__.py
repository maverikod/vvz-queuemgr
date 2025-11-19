"""
Job classes for the queue manager system.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from .base import QueueJobBase
from .registry_job import RegistryPlaceholderJob

__all__ = ["QueueJobBase", "RegistryPlaceholderJob"]
