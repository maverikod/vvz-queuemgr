"""
Registry loading helpers for ``JobQueue``.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict

from queuemgr.core.registry import Registry
from queuemgr.core.types import JobId
from queuemgr.jobs.base import QueueJobBase
from queuemgr.jobs.registry_job import RegistryPlaceholderJob


def load_jobs_from_registry(
    registry: Registry,
    jobs: Dict[JobId, QueueJobBase],
    job_creation_times: Dict[JobId, datetime],
    job_types: Dict[JobId, str],
    logger: logging.Logger,
) -> None:
    """
    Populate in-memory structures with the latest snapshot from the registry.

    Args:
        registry: Registry providing ``all_latest`` iterator.
        jobs: Mutable job dictionary to fill with placeholder jobs.
        job_creation_times: Mapping of job id to creation timestamp.
        job_types: Mapping of job id to a textual job type label.
        logger: Logger used for debug/warning messages.
    """
    try:
        for record in registry.all_latest():
            job_id = record.job_id
            if job_id in jobs:
                continue

            placeholder_job = RegistryPlaceholderJob(job_id, record)
            jobs[job_id] = placeholder_job
            job_creation_times[job_id] = record.created_at
            job_types[job_id] = getattr(record, "job_type", "RegistryPlaceholderJob")

            logger.debug(
                "Loaded job %s from registry (status: %s)",
                job_id,
                record.status.name,
            )
    except Exception as error:
        logger.warning(
            "Failed to load jobs from registry: %s. Continuing with current state.",
            error,
        )
