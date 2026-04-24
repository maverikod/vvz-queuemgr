"""
Metrics and inspection helpers for ``JobQueue``.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from queuemgr.core.types import JobId, JobStatus
from queuemgr.jobs.base import QueueJobBase
from queuemgr.exceptions import JobNotFoundError
from queuemgr.queue.terminal_status import is_terminal_job_status


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

    def get_job_type_name(self, job_id: JobId) -> Optional[str]:
        """
        Return the class name used as job type for the given id.

        Args:
            job_id: Job identifier.

        Returns:
            Registered type label, or None when unknown.
        """
        return self._job_types.get(job_id)

    def get_job_by_id(self, job_id: JobId) -> Optional[QueueJobBase]:
        """
        Lookup job instance without raising ``JobNotFoundError``.

        Args:
            job_id: Identifier to search for.

        Returns:
            Job instance when found, otherwise ``None``.
        """
        return self._jobs.get(job_id)

    def _retention_seconds_for_status(self, status: JobStatus) -> float:
        """
        Return TTL (seconds) for a terminal status, or infinity when disabled.

        Args:
            status: Terminal job status.

        Returns:
            Retention duration in seconds; ``math.inf`` means no TTL eviction.
        """
        if status == JobStatus.ERROR:
            ttl = getattr(self, "_retention_ttl_failed", math.inf)
        elif status == JobStatus.STOPPED:
            ttl = getattr(self, "_retention_ttl_stopped", math.inf)
        elif status == JobStatus.DELETED:
            ttl = getattr(self, "_retention_ttl_deleted", math.inf)
        else:
            ttl = getattr(self, "_retention_ttl_completed", math.inf)
        if ttl is None:
            return math.inf
        return float(ttl)

    def _terminal_timestamp(self, job_id: JobId) -> Optional[datetime]:
        """
        Return when the job first reached a terminal state, if known.

        Args:
            job_id: Job identifier.

        Returns:
            Timestamp or None when not yet recorded.
        """
        terminal_at = getattr(self, "_job_terminal_at", {}).get(job_id)
        if terminal_at is not None:
            return terminal_at
        if hasattr(self, "_job_completed_times"):
            return self._job_completed_times.get(job_id)
        return None

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
        Remove terminal jobs past retention TTL or over max_retained_terminal_jobs.

        Pending and running jobs are never removed. Uses per-status TTL attributes
        on ``JobQueue`` (``_retention_ttl_*``) and ``max_retained_terminal_jobs``.

        Returns:
            Number of jobs purged from the in-memory queue.
        """
        purge = getattr(self, "_purge_job_immediately", None)
        if purge is None:
            return 0

        now = datetime.now()
        terminal_rows: List[Tuple[JobId, JobStatus, datetime]] = []

        for job_id, job in self._jobs.items():
            status_data = job.get_status()
            status = status_data["status"]
            if not is_terminal_job_status(status):
                continue
            t_at = self._terminal_timestamp(job_id)
            if t_at is None:
                t_at = self._job_creation_times.get(job_id, now)
            terminal_rows.append((job_id, status, t_at))

        jobs_to_remove: List[JobId] = []

        for job_id, status, t_at in terminal_rows:
            ttl = self._retention_seconds_for_status(status)
            if not math.isinf(ttl):
                age = (now - t_at).total_seconds()
                if age >= ttl:
                    jobs_to_remove.append(job_id)

        survivors = [(j, s, t) for j, s, t in terminal_rows if j not in jobs_to_remove]
        max_retained = int(getattr(self, "max_retained_terminal_jobs", 1000))
        if len(survivors) > max_retained:
            survivors_sorted = sorted(survivors, key=lambda row: row[2])
            overflow = len(survivors) - max_retained
            for job_id, _, _ in survivors_sorted[:overflow]:
                jobs_to_remove.append(job_id)

        removed = 0
        for job_id in set(jobs_to_remove):
            try:
                purge(job_id)
                removed += 1
            except JobNotFoundError:
                continue
        return removed

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

    def get_job_logs(self, job_id: JobId) -> Dict[str, List[str]]:
        """
        Get stdout and stderr logs for a job.

        Args:
            job_id: Job identifier to look up.

        Returns:
            Dictionary containing:
            - stdout: List of stdout lines
            - stderr: List of stderr lines

        Raises:
            JobNotFoundError: If job is not found.
        """
        if job_id not in self._jobs:
            raise JobNotFoundError(job_id)

        job = self._jobs[job_id]
        if job._shared_state is None:
            return {"stdout": [], "stderr": []}

        stdout_logs = job._shared_state.get("stdout")
        stderr_logs = job._shared_state.get("stderr")

        # Return copies of the lists to avoid race conditions
        stdout = list(stdout_logs) if stdout_logs is not None else []
        stderr = list(stderr_logs) if stderr_logs is not None else []

        return {"stdout": stdout, "stderr": stderr}
