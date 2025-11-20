"""
Metrics and inspection helpers for ``JobQueue``.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from queuemgr.core.types import JobId, JobStatus
from queuemgr.jobs.base import QueueJobBase


class JobQueueMetricsMixin:
    """
    Provides read-only inspection helpers shared by JobQueue implementations.

    The mixin assumes inheriting classes define ``_jobs``, ``_job_creation_times``,
    and ``_job_types`` attributes mirroring the main queue state.
    """

    _jobs: Dict[JobId, QueueJobBase]
    _job_creation_times: Dict[JobId, datetime]
    _job_types: Dict[JobId, str]

    def get_job_count(self) -> int:
        """
        Return total number of jobs currently tracked in memory.

        Returns:
            Number of jobs stored in the queue.
        """
        return len(self._jobs)

    def get_running_jobs(self) -> Dict[JobId, QueueJobBase]:
        """
        Return dictionary of all jobs whose worker processes are running.

        Returns:
            Mapping of job IDs to job instances that report ``is_running()``.
        """
        return {job_id: job for job_id, job in self._jobs.items() if job.is_running()}

    def get_job_by_id(self, job_id: JobId) -> Optional[QueueJobBase]:
        """
        Lookup job instance without raising ``JobNotFoundError``.

        Args:
            job_id: Identifier to search for.

        Returns:
            Job instance when found, otherwise ``None``.
        """
        return self._jobs.get(job_id)

    def list_job_statuses(self) -> Dict[JobId, JobStatus]:
        """
        Produce a mapping of job IDs to their current ``JobStatus`` values.

        Returns:
            Dictionary of job IDs and statuses as reported by ``get_status``.
        """
        statuses: Dict[JobId, JobStatus] = {}
        for job_id, job in self._jobs.items():
            status_data = job.get_status()
            statuses[job_id] = status_data["status"]
        return statuses

    def cleanup_completed_jobs(self) -> int:
        """
        Remove jobs that finished with COMPLETED or ERROR statuses.

        Returns:
            Number of jobs removed from the in-memory queue.
        """
        jobs_to_remove = []
        for job_id, job in self._jobs.items():
            status_data = job.get_status()
            if status_data["status"] in [JobStatus.COMPLETED, JobStatus.ERROR]:
                jobs_to_remove.append(job_id)

        for job_id in jobs_to_remove:
            del self._jobs[job_id]
            if hasattr(self, "_job_creation_times"):
                if job_id in self._job_creation_times:
                    del self._job_creation_times[job_id]
            if hasattr(self, "_job_types") and job_id in self._job_types:
                del self._job_types[job_id]

        return len(jobs_to_remove)

    def _get_jobs_by_type(self, job_type: str) -> List[JobId]:
        """
        Return identifiers of jobs matching the provided type label.

        Args:
            job_type: Class name associated with the jobs of interest.

        Returns:
            List of job IDs registered under the specified type.
        """
        return [
            job_id
            for job_id, registered_type in self._job_types.items()
            if registered_type == job_type
        ]

    def _find_oldest_job_id(self, job_ids: List[JobId]) -> Optional[JobId]:
        """
        Determine the oldest job identifier among the provided list.

        Args:
            job_ids: Collection of job identifiers to evaluate.

        Returns:
            Job ID with the earliest creation timestamp, or None when empty.
        """
        if not job_ids:
            return None
        return min(
            job_ids,
            key=lambda jid: self._job_creation_times.get(jid, datetime.now()),
        )
