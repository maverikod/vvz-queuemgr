"""
Job queue implementation for managing job lifecycle and IPC state.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Type, Union

from queuemgr.core.types import JobId, JobRecord, JobStatus, JobCommand
from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.registry import Registry, InMemoryRegistry
from queuemgr.core.ipc import get_manager, create_job_shared_state, set_command
from queuemgr.exceptions import (
    JobNotFoundError,
    JobAlreadyExistsError,
    InvalidJobStateError,
    ProcessControlError,
)
from .job_registry_loader import load_jobs_from_registry
from .job_queue_limits import enforce_global_limit, enforce_per_type_limit
from .job_queue_metrics import JobQueueMetricsMixin

logger = logging.getLogger("queuemgr.queue.job_queue")


class JobQueue(JobQueueMetricsMixin):
    """
    Coordinator for job lifecycle and IPC state. Provides dictionary of jobs,
    status lookup, and job operations (add, delete, start, stop, suspend).
    """

    def __init__(
        self,
        registry: Optional[Registry] = None,
        max_queue_size: Optional[int] = None,
        per_job_type_limits: Optional[Dict[str, int]] = None,
        completed_job_retention_seconds: Optional[float] = None,
    ) -> None:
        """
        Initialize the job queue.

        Args:
            registry: Registry instance for persisting job states.
            max_queue_size: Global maximum number of jobs (optional).
            per_job_type_limits: Dict mapping job_type to max count (optional).
            completed_job_retention_seconds: How long to keep completed/error jobs
                before auto-removal (optional). If None, completed jobs are preserved.
        """
        self.registry = registry or InMemoryRegistry()
        self._registry = self.registry  # Backward-compatibility alias
        self._jobs: Dict[JobId, QueueJobBase] = {}
        self._manager = get_manager()
        self._job_creation_times: Dict[JobId, datetime] = {}
        self._job_started_times: Dict[JobId, datetime] = {}
        self._job_completed_times: Dict[JobId, datetime] = {}
        self._job_types: Dict[JobId, str] = {}
        self.max_queue_size = max_queue_size
        self.per_job_type_limits = per_job_type_limits or {}
        self.completed_job_retention_seconds = completed_job_retention_seconds
        load_jobs_from_registry(
            registry=self.registry,
            jobs=self._jobs,
            job_creation_times=self._job_creation_times,
            job_types=self._job_types,
            logger=logger,
        )

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
            status_text = (
                status_value.name
                if isinstance(status_value, JobStatus)
                else str(status_value)
            )
            created_at = self._job_creation_times.get(job_id, datetime.now())
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
        current_status = status_data["status"]

        created_at = self._job_creation_times.get(job_id, datetime.now())
        started_at = self._job_started_times.get(job_id)
        completed_at = self._job_completed_times.get(job_id)

        # Update started_at if job is running but not tracked yet
        if current_status == JobStatus.RUNNING and started_at is None:
            started_at = datetime.now()
            self._job_started_times[job_id] = started_at

        # Update completed_at if job is completed/error but not tracked yet
        is_completed = current_status in [JobStatus.COMPLETED, JobStatus.ERROR]
        if is_completed and completed_at is None:
            completed_at = datetime.now()
            self._job_completed_times[job_id] = completed_at

        return JobRecord(
            job_id=job_id,
            status=current_status,
            progress=status_data["progress"],
            description=status_data["description"],
            result=status_data["result"],
            created_at=created_at,
            updated_at=datetime.now(),
            started_at=started_at,
            completed_at=completed_at,
        )

    def add_job(
        self,
        job: Union[QueueJobBase, Type[QueueJobBase]],
        job_id: Optional[JobId] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> JobId:
        """
        Add a new job; returns its job_id. Initial state is PENDING.

        If per-job-type limits are configured and limit is reached,
        the oldest job of this type will be removed first (FIFO eviction).

        Args:
            job: Job instance or QueueJobBase subclass to add.
            job_id: Job identifier when providing a class instead of an instance.
            params: Parameters to pass to the job constructor when a class is provided.

        Returns:
            Job ID of the added job.

        Raises:
            JobAlreadyExistsError: If job with same ID already exists.
        """
        if isinstance(job, type):
            if not issubclass(job, QueueJobBase):
                raise TypeError("job must be a QueueJobBase subclass")
            if job_id is None:
                raise ValueError("job_id must be provided when adding by class")
            job_instance = job(job_id, params or {})
        else:
            job_instance = job

        if job_instance.job_id in self._jobs:
            raise JobAlreadyExistsError(job_instance.job_id)

        # Determine job type from class name
        job_type = job_instance.__class__.__name__

        def delete_job_callback(target_job_id: JobId) -> None:
            """Internal helper to delete jobs during eviction."""
            self.delete_job(target_job_id, force=True)

        enforce_per_type_limit(
            job_type=job_type,
            new_job_id=job_instance.job_id,
            per_job_type_limits=self.per_job_type_limits,
            job_types=self._job_types,
            job_creation_times=self._job_creation_times,
            delete_callback=delete_job_callback,
            logger=logger,
        )

        enforce_global_limit(
            new_job_id=job_instance.job_id,
            jobs=self._jobs,
            job_creation_times=self._job_creation_times,
            max_queue_size=self.max_queue_size,
            delete_callback=delete_job_callback,
            logger=logger,
        )

        # Set up shared state for the job
        shared_state = create_job_shared_state(self._manager)
        job_instance._set_shared_state(shared_state)
        job_instance._set_registry(self.registry)

        # Add to jobs dictionary
        self._jobs[job_instance.job_id] = job_instance
        self._job_creation_times[job_instance.job_id] = datetime.now()
        self._job_types[job_instance.job_id] = job_type

        # Write initial state to registry
        initial_record = JobRecord(
            job_id=job_instance.job_id,
            status=JobStatus.PENDING,
            progress=0,
            description="Job created",
            result=None,
            created_at=self._job_creation_times[job_instance.job_id],
            updated_at=datetime.now(),
        )
        self.registry.append(initial_record)

        return job_instance.job_id

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
                # Record start time
                self._job_started_times[job_id] = datetime.now()
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
