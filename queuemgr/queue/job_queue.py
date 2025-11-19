"""
Job queue implementation for managing job lifecycle and IPC state.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from queuemgr.core.types import JobId, JobRecord, JobStatus, JobCommand
from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.registry import Registry
from queuemgr.core.ipc import get_manager, create_job_shared_state, set_command
from .exceptions import (
    JobNotFoundError,
    JobAlreadyExistsError,
    InvalidJobStateError,
)
from ..core.exceptions import ProcessControlError

logger = logging.getLogger("queuemgr.queue.job_queue")


class JobQueue:
    """
    Coordinator for job lifecycle and IPC state. Provides dictionary of jobs,
    status lookup, and job operations (add, delete, start, stop, suspend).
    """

    def __init__(
        self,
        registry: Registry,
        max_queue_size: Optional[int] = None,
        per_job_type_limits: Optional[Dict[str, int]] = None,
    ) -> None:
        """
        Initialize the job queue.

        Args:
            registry: Registry instance for persisting job states.
            max_queue_size: Global maximum number of jobs (optional).
            per_job_type_limits: Dict mapping job_type to max count (optional).
        """
        self.registry = registry
        self._jobs: Dict[JobId, QueueJobBase] = {}
        self._manager = get_manager()
        self._job_creation_times: Dict[JobId, datetime] = {}
        self._job_types: Dict[JobId, str] = {}
        self.max_queue_size = max_queue_size
        self.per_job_type_limits = per_job_type_limits or {}

    def get_jobs(self) -> Mapping[JobId, QueueJobBase]:
        """
        Return a read-only mapping of job_id -> job instance.

        Returns:
            Read-only mapping of job IDs to job instances.
        """
        return self._jobs.copy()

    def list_jobs(self) -> List[Dict[str, Any]]:
        """
        Return a JSON-serializable snapshot for every queued job.

        Returns:
            List of dictionaries describing each job (job_id, status, progress,
            metadata) that can be safely serialized to JSON for IPC responses.
        """
        job_snapshots: List[Dict[str, Any]] = []

        for job_id, job in self._jobs.items():
            status_data = job.get_status()
            status_value = status_data.get("status", JobStatus.PENDING)
            created_at = self._job_creation_times.get(job_id, datetime.now())

            if isinstance(status_value, JobStatus):
                status_text = status_value.name
            else:
                status_text = str(status_value)

            job_snapshots.append(
                {
                    "job_id": job_id,
                    "status": status_text,
                    "progress": int(status_data.get("progress", 0)),
                    "description": status_data.get("description", ""),
                    "result": status_data.get("result"),
                    "is_running": job.is_running(),
                    "created_at": created_at.isoformat(),
                    "updated_at": datetime.now().isoformat(),
                }
            )

        return job_snapshots

    def get_job_status(self, job_id: JobId) -> JobRecord:
        """
        Return status, progress, description, and latest result for a job.

        Args:
            job_id: Job identifier to look up.

        Returns:
            JobRecord containing current job state.

        Raises:
            JobNotFoundError: If job is not found.
        """
        if job_id not in self._jobs:
            raise JobNotFoundError(job_id)

        job = self._jobs[job_id]
        status_data = job.get_status()

        return JobRecord(
            job_id=job_id,
            status=status_data["status"],
            progress=status_data["progress"],
            description=status_data["description"],
            result=status_data["result"],
            created_at=self._job_creation_times[job_id],
            updated_at=datetime.now(),
        )

    def add_job(self, job: QueueJobBase) -> JobId:
        """
        Add a new job; returns its job_id. Initial state is PENDING.

        If per-job-type limits are configured and limit is reached,
        the oldest job of this type will be removed first (FIFO eviction).

        Args:
            job: Job instance to add.

        Returns:
            Job ID of the added job.

        Raises:
            JobAlreadyExistsError: If job with same ID already exists.
        """
        if job.job_id in self._jobs:
            raise JobAlreadyExistsError(job.job_id)

        # Determine job type from class name
        job_type = job.__class__.__name__

        # Check per-job-type limit and evict if necessary
        if self.per_job_type_limits and job_type in self.per_job_type_limits:
            limit = self.per_job_type_limits[job_type]
            existing_jobs = self._get_jobs_by_type(job_type)

            if len(existing_jobs) >= limit:
                # Find and remove oldest job of this type
                oldest_job_id = self._find_oldest_job_id(existing_jobs)
                if oldest_job_id:
                    logger.info(
                        f"Evicting oldest {job_type} job {oldest_job_id} "
                        f"(limit: {limit}, adding: {job.job_id})"
                    )
                    self.delete_job(oldest_job_id, force=True)

        # Check global queue size limit if configured
        if self.max_queue_size and len(self._jobs) >= self.max_queue_size:
            # Find oldest job overall
            if self._jobs:
                oldest_job_id = min(
                    self._jobs.keys(),
                    key=lambda jid: self._job_creation_times.get(jid, datetime.now()),
                )
                logger.info(
                    f"Evicting oldest job {oldest_job_id} "
                    f"(global limit: {self.max_queue_size}, adding: {job.job_id})"
                )
                self.delete_job(oldest_job_id, force=True)

        # Set up shared state for the job
        shared_state = create_job_shared_state(self._manager)
        job._set_shared_state(shared_state)
        job._set_registry(self.registry)

        # Add to jobs dictionary
        self._jobs[job.job_id] = job
        self._job_creation_times[job.job_id] = datetime.now()
        self._job_types[job.job_id] = job_type

        # Write initial state to registry
        initial_record = JobRecord(
            job_id=job.job_id,
            status=JobStatus.PENDING,
            progress=0,
            description="Job created",
            result=None,
            created_at=self._job_creation_times[job.job_id],
            updated_at=datetime.now(),
        )
        self.registry.append(initial_record)

        return job.job_id

    def delete_job(self, job_id: JobId, force: bool = False) -> None:
        """
        Delete job; if running, request STOP or terminate if force=True.

        Args:
            job_id: Job identifier to delete.
            force: If True, forcefully terminate running job.

        Raises:
            JobNotFoundError: If job is not found.
            ProcessControlError: If process control fails.
        """
        if job_id not in self._jobs:
            raise JobNotFoundError(job_id)

        job = self._jobs[job_id]

        try:
            if job.is_running():
                if force:
                    job.terminate_process()
                else:
                    job.stop_process(timeout=10.0)  # 10 second timeout
        except ProcessControlError:
            if not force:
                raise
            # If force=True, try to terminate anyway
            try:
                job.terminate_process()
            except ProcessControlError:
                pass  # Ignore errors when force deleting

        # Remove from jobs dictionary
        del self._jobs[job_id]
        del self._job_creation_times[job_id]
        if job_id in self._job_types:
            del self._job_types[job_id]

        # Write deletion record to registry
        deletion_record = JobRecord(
            job_id=job_id,
            status=JobStatus.INTERRUPTED,
            progress=0,
            description="Job deleted",
            result=None,
            created_at=self._job_creation_times.get(job_id, datetime.now()),
            updated_at=datetime.now(),
        )
        self.registry.append(deletion_record)

    def start_job(self, job_id: JobId) -> None:
        """
        Start job execution in a new child process.

        Args:
            job_id: Job identifier to start.

        Raises:
            JobNotFoundError: If job is not found.
            InvalidJobStateError: If job is not in a startable state.
            ProcessControlError: If process creation fails.
        """
        if job_id not in self._jobs:
            raise JobNotFoundError(job_id)

        job = self._jobs[job_id]
        current_status = job.get_status()["status"]

        # Check if job can be started
        if current_status not in [JobStatus.PENDING, JobStatus.INTERRUPTED]:
            raise InvalidJobStateError(job_id, current_status.name, "start")

        if job.is_running():
            raise InvalidJobStateError(job_id, "RUNNING", "start")

        try:
            job.start_process()
            # Send START command to the job

            if job._shared_state is not None:
                set_command(job._shared_state, JobCommand.START)
        except ProcessControlError as e:
            raise ProcessControlError(job_id, "start", e)

    def stop_job(self, job_id: JobId, timeout: Optional[float] = None) -> None:
        """
        Request graceful STOP and wait up to timeout.

        Args:
            job_id: Job identifier to stop.
            timeout: Maximum time to wait for graceful stop (seconds).

        Raises:
            JobNotFoundError: If job is not found.
            ProcessControlError: If stop fails.
        """
        if job_id not in self._jobs:
            raise JobNotFoundError(job_id)

        job = self._jobs[job_id]

        if not job.is_running():
            return  # Job not running, nothing to stop

        try:
            job.stop_process(timeout=timeout)
        except ProcessControlError as e:
            raise ProcessControlError(job_id, "stop", e)

    def suspend_job(self, job_id: JobId) -> None:
        """
        Optional: mark as paused (if supported).

        For now, this is equivalent to stopping the job.

        Args:
            job_id: Job identifier to suspend.

        Raises:
            JobNotFoundError: If job is not found.
        """
        self.stop_job(job_id)

    def get_job_count(self) -> int:
        """
        Get the total number of jobs in the queue.

        Returns:
            Number of jobs in the queue.
        """
        return len(self._jobs)

    def get_running_jobs(self) -> Dict[JobId, QueueJobBase]:
        """
        Get all currently running jobs.

        Returns:
            Dictionary of running job IDs to job instances.
        """
        running_jobs = {}
        for job_id, job in self._jobs.items():
            if job.is_running():
                running_jobs[job_id] = job
        return running_jobs

    def get_job_by_id(self, job_id: JobId) -> Optional[QueueJobBase]:
        """
        Get a job by its ID.

        Args:
            job_id: Job identifier to look up.

        Returns:
            Job instance if found, None otherwise.
        """
        return self._jobs.get(job_id)

    def list_job_statuses(self) -> Dict[JobId, JobStatus]:
        """
        Get the status of all jobs.

        Returns:
            Dictionary mapping job IDs to their current status.
        """
        statuses = {}
        for job_id, job in self._jobs.items():
            status_data = job.get_status()
            statuses[job_id] = status_data["status"]
        return statuses

    def cleanup_completed_jobs(self) -> int:
        """
        Remove completed and error jobs from the queue.

        Returns:
            Number of jobs removed.
        """
        jobs_to_remove = []

        for job_id, job in self._jobs.items():
            status_data = job.get_status()
            if status_data["status"] in [JobStatus.COMPLETED, JobStatus.ERROR]:
                jobs_to_remove.append(job_id)

        for job_id in jobs_to_remove:
            del self._jobs[job_id]
            if job_id in self._job_creation_times:
                del self._job_creation_times[job_id]
            if job_id in self._job_types:
                del self._job_types[job_id]

        return len(jobs_to_remove)

    def shutdown(self, timeout: float = 30.0) -> None:
        """
        Shutdown the queue, stopping all running jobs.

        Args:
            timeout: Maximum time to wait for jobs to stop gracefully.

        Raises:
            ProcessControlError: If some jobs fail to stop.
        """
        running_jobs = self.get_running_jobs()

        # First, try to stop all jobs gracefully
        for job_id, job in running_jobs.items():
            try:
                job.stop_process(
                    timeout=(timeout / len(running_jobs) if running_jobs else timeout)
                )
            except ProcessControlError:
                # If graceful stop fails, force terminate
                try:
                    job.terminate_process()
                except ProcessControlError:
                    pass  # Ignore errors during shutdown

        # Clear all jobs
        self._jobs.clear()
        self._job_creation_times.clear()
        self._job_types.clear()

    def _get_jobs_by_type(self, job_type: str) -> List[JobId]:
        """
        Get all job IDs of a specific type.

        Args:
            job_type: Job type to filter by.

        Returns:
            List of job IDs of the specified type.
        """
        return [
            job_id for job_id, jtype in self._job_types.items() if jtype == job_type
        ]

    def _find_oldest_job_id(self, job_ids: List[JobId]) -> Optional[JobId]:
        """
        Find the oldest job ID by creation time.

        Args:
            job_ids: List of job IDs to search.

        Returns:
            Oldest job ID, or None if list is empty.
        """
        if not job_ids:
            return None

        return min(
            job_ids,
            key=lambda jid: self._job_creation_times.get(jid, datetime.now()),
        )
