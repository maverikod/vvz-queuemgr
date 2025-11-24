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
from queuemgr.exceptions import JobNotFoundError


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

        Jobs are removed only if:
        1. They have been completed/errored for longer than
           completed_job_retention_seconds (if configured), OR
        2. Limits are configured and cleanup is needed for space.

        If completed_job_retention_seconds is None and no limits are set,
        completed jobs are preserved to allow clients to retrieve results.

        Returns:
            Number of jobs removed from the in-memory queue.
        """
        from datetime import datetime

        retention_seconds = getattr(self, "completed_job_retention_seconds", None)
        has_limits = False
        if hasattr(self, "max_queue_size") and self.max_queue_size is not None:
            has_limits = True
        if hasattr(self, "per_job_type_limits") and self.per_job_type_limits:
            has_limits = True

        # If no retention and no limits, preserve completed jobs
        if retention_seconds is None and not has_limits:
            return 0

        now = datetime.now()
        jobs_to_remove = []

        for job_id, job in self._jobs.items():
            status_data = job.get_status()
            status = status_data["status"]

            if status not in [JobStatus.COMPLETED, JobStatus.ERROR]:
                continue

            # Check if job should be removed based on retention time
            completed_at = None
            if hasattr(self, "_job_completed_times"):
                completed_at = self._job_completed_times.get(job_id)

            # If completed_at is not set yet, try to get it from job status
            if completed_at is None:
                # Job just completed, don't remove it yet
                continue

            # Check retention time
            if retention_seconds is not None:
                time_since_completion = (now - completed_at).total_seconds()
                if time_since_completion >= retention_seconds:
                    jobs_to_remove.append(job_id)
                    continue

            # If limits are set and we need space, remove old completed jobs
            if has_limits:
                # Only remove if retention allows
                # (or if retention is None and we need space)
                time_since = (now - completed_at).total_seconds()
                if retention_seconds is None or (
                    retention_seconds is not None and time_since >= retention_seconds
                ):
                    jobs_to_remove.append(job_id)

        for job_id in jobs_to_remove:
            del self._jobs[job_id]
            if hasattr(self, "_job_creation_times"):
                if job_id in self._job_creation_times:
                    del self._job_creation_times[job_id]
            if hasattr(self, "_job_started_times"):
                if job_id in self._job_started_times:
                    del self._job_started_times[job_id]
            if hasattr(self, "_job_completed_times"):
                if job_id in self._job_completed_times:
                    del self._job_completed_times[job_id]
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
