"""
Job queue implementation for managing job lifecycle and IPC state.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Mapping, Optional

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


class JobQueue:
    """
    Coordinator for job lifecycle and IPC state. Provides dictionary of jobs,
    status lookup, and job operations (add, delete, start, stop, suspend).
    """

    def __init__(self, registry: Registry) -> None:
        """
        Initialize the job queue.

        Args:
            registry: Registry instance for persisting job states.
        """
        self.registry = registry
        self._jobs: Dict[JobId, QueueJobBase] = {}
        self._manager = get_manager()
        self._job_creation_times: Dict[JobId, datetime] = {}

    def get_jobs(self) -> Mapping[JobId, QueueJobBase]:
        """
        Return a read-only mapping of job_id -> job instance.

        Returns:
            Read-only mapping of job IDs to job instances.
        """
        return self._jobs.copy()

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

        Args:
            job: Job instance to add.

        Returns:
            Job ID of the added job.

        Raises:
            JobAlreadyExistsError: If job with same ID already exists.
        """
        if job.job_id in self._jobs:
            raise JobAlreadyExistsError(job.job_id)

        # Set up shared state for the job
        shared_state = create_job_shared_state(self._manager)
        job._set_shared_state(shared_state)
        job._set_registry(self.registry)

        # Add to jobs dictionary
        self._jobs[job.job_id] = job
        self._job_creation_times[job.job_id] = datetime.now()

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
