"""
Limit enforcement helpers for ``JobQueue``.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional

from queuemgr.core.types import JobId
from queuemgr.jobs.base import QueueJobBase

DeleteCallback = Callable[[JobId], None]


def enforce_per_type_limit(
    job_type: str,
    new_job_id: JobId,
    per_job_type_limits: Dict[str, int],
    job_types: Dict[JobId, str],
    job_creation_times: Dict[JobId, datetime],
    delete_callback: DeleteCallback,
    logger: logging.Logger,
) -> None:
    """
    Ensure per-type limit is respected, evicting the oldest job if needed.

    Args:
        job_type: Type for the job being added.
        new_job_id: Identifier of the job being added.
        per_job_type_limits: Mapping of job type to max in-memory count.
        job_types: Mapping of current job ids to their types.
        job_creation_times: Mapping of job ids to creation timestamps.
        delete_callback: Callable used to delete a job by id.
        logger: Logger for eviction messages.
    """
    if job_type not in per_job_type_limits:
        return

    limit = per_job_type_limits[job_type]
    existing_jobs = _get_jobs_by_type(job_types, job_type)

    if len(existing_jobs) < limit:
        return

    oldest_job_id = _find_oldest_job_id(existing_jobs, job_creation_times)
    if not oldest_job_id:
        return

    logger.info(
        "Evicting oldest %s job %s (limit: %s, adding: %s)",
        job_type,
        oldest_job_id,
        limit,
        new_job_id,
    )
    delete_callback(oldest_job_id)


def enforce_global_limit(
    new_job_id: JobId,
    jobs: Dict[JobId, QueueJobBase],
    job_creation_times: Dict[JobId, datetime],
    max_queue_size: Optional[int],
    delete_callback: DeleteCallback,
    logger: logging.Logger,
) -> None:
    """
    Enforce global queue size limit using FIFO eviction.

    Args:
        new_job_id: Identifier of job being added.
        jobs: Active job dictionary.
        job_creation_times: Mapping of job ids to creation timestamps.
        max_queue_size: Maximum allowed number of jobs.
        delete_callback: Callable used to delete a job by id.
        logger: Logger for eviction messages.
    """
    if not max_queue_size:
        return

    if len(jobs) < max_queue_size:
        return

    if not jobs:
        return

    oldest_job_id = min(
        jobs.keys(),
        key=lambda jid: job_creation_times.get(jid, datetime.now()),
    )
    logger.info(
        "Evicting oldest job %s (global limit: %s, adding: %s)",
        oldest_job_id,
        max_queue_size,
        new_job_id,
    )
    delete_callback(oldest_job_id)


def _get_jobs_by_type(job_types: Dict[JobId, str], job_type: str) -> List[JobId]:
    """Return list of job ids matching a particular job type."""
    return [job_id for job_id, jtype in job_types.items() if jtype == job_type]


def _find_oldest_job_id(
    job_ids: List[JobId],
    job_creation_times: Dict[JobId, datetime],
) -> Optional[JobId]:
    """Return the oldest job id according to tracked creation times."""
    if not job_ids:
        return None
    return min(job_ids, key=lambda jid: job_creation_times.get(jid, datetime.now()))
