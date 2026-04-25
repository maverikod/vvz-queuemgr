"""
Job queue implementation for managing job lifecycle and IPC state.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple, Type, Union

from queuemgr.constants import DESCRIPTION_JOB_STOPPED
from queuemgr.core.types import (
    JobId,
    JobRecord,
    JobStatus,
    JobCommand,
    normalize_public_job_status,
    public_status_name,
)
from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.registry import Registry, InMemoryRegistry
from queuemgr.core.ipc import get_manager, create_job_shared_state, set_command
from queuemgr.core.ipc_operations import update_job_state
from queuemgr.queue.terminal_status import is_terminal_job_status
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
        terminal_job_retention_seconds: Optional[float] = None,
        failed_terminal_retention_seconds: Optional[float] = None,
        stopped_terminal_retention_seconds: Optional[float] = None,
        deleted_terminal_retention_seconds: Optional[float] = None,
        max_retained_terminal_jobs: int = 1000,
    ) -> None:
        """
        Initialize the job queue.

        Args:
            registry: Registry instance for persisting job states.
            max_queue_size: Global maximum number of jobs (optional).
            per_job_type_limits: Dict mapping job_type to max count (optional).
            completed_job_retention_seconds: Legacy TTL for completed and error jobs
                when set; overrides per-status defaults for backward compatibility.
            terminal_job_retention_seconds: TTL for completed and stopped jobs.
            failed_terminal_retention_seconds: TTL for ERROR jobs (default 86400s).
            stopped_terminal_retention_seconds: TTL for STOPPED jobs; defaults to
                terminal_job_retention_seconds when omitted.
            deleted_terminal_retention_seconds: TTL for DELETED jobs; defaults to
                terminal_job_retention_seconds when omitted.
            max_retained_terminal_jobs: Maximum terminal jobs kept in memory.
        """
        self.registry = registry or InMemoryRegistry()
        self._registry = self.registry  # Backward-compatibility alias
        self._jobs: Dict[JobId, QueueJobBase] = {}
        self._manager = get_manager()
        self._job_creation_times: Dict[JobId, datetime] = {}
        self._job_started_times: Dict[JobId, datetime] = {}
        self._job_completed_times: Dict[JobId, datetime] = {}
        self._job_terminal_at: Dict[JobId, datetime] = {}
        self._job_types: Dict[JobId, str] = {}
        self.max_queue_size = max_queue_size
        self.per_job_type_limits = per_job_type_limits or {}
        self.completed_job_retention_seconds = completed_job_retention_seconds
        self.max_retained_terminal_jobs = max_retained_terminal_jobs

        term = (
            float(terminal_job_retention_seconds)
            if terminal_job_retention_seconds is not None
            else 3600.0
        )
        failed = (
            float(failed_terminal_retention_seconds)
            if failed_terminal_retention_seconds is not None
            else 86400.0
        )
        if completed_job_retention_seconds is not None:
            term = float(completed_job_retention_seconds)
            failed = float(completed_job_retention_seconds)
        self._retention_ttl_completed = term
        self._retention_ttl_failed = failed
        self._retention_ttl_stopped = (
            float(stopped_terminal_retention_seconds)
            if stopped_terminal_retention_seconds is not None
            else term
        )
        self._retention_ttl_deleted = (
            float(deleted_terminal_retention_seconds)
            if deleted_terminal_retention_seconds is not None
            else term
        )

        load_jobs_from_registry(
            registry=self.registry,
            jobs=self._jobs,
            job_creation_times=self._job_creation_times,
            job_types=self._job_types,
            logger=logger,
        )

    def _purge_job_immediately(self, job_id: JobId) -> None:
        """
        Hard-remove a job from memory (used for retention cleanup and limit eviction).

        Args:
            job_id: Identifier to remove.

        Raises:
            JobNotFoundError: If the job is not tracked.
        """
        if job_id not in self._jobs:
            raise JobNotFoundError(job_id)
        job = self._jobs[job_id]
        try:
            if job.is_running():
                try:
                    job.terminate_process()
                except ProcessControlError:
                    pass
        except ProcessControlError:
            pass
        del self._jobs[job_id]
        self._job_creation_times.pop(job_id, None)
        self._job_started_times.pop(job_id, None)
        self._job_completed_times.pop(job_id, None)
        self._job_terminal_at.pop(job_id, None)
        self._job_types.pop(job_id, None)

    def _evict_oldest_terminal_job(self) -> bool:
        """
        Purge the single oldest terminal job to free a queue slot.

        Returns:
            True if a job was removed, False when no terminal job is available.
        """
        terminals: List[Tuple[datetime, JobId]] = []
        for jid, job in self._jobs.items():
            st = job.get_status()["status"]
            if is_terminal_job_status(st):
                t_at = self._job_terminal_at.get(jid) or self._job_completed_times.get(
                    jid
                )
                if t_at is None:
                    t_at = self._job_creation_times.get(jid, datetime.now())
                terminals.append((t_at, jid))
        if not terminals:
            return False
        oldest_id = min(terminals, key=lambda x: x[0])[1]
        self._purge_job_immediately(oldest_id)
        return True

    def get_jobs(self) -> Mapping[JobId, QueueJobBase]:
        """
        Return a read-only mapping of job_id -> job instance.

        Returns:
            Read-only mapping of job IDs to job instances.
        """
        return self._jobs.copy()

    def list_jobs(self, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Return a JSON-serializable snapshot for every queued job.

        Args:
            status_filter: When set, only jobs whose status name matches (case-folded).

        Returns:
            List of dictionaries describing each job (job_id, status, progress,
            metadata) that can be safely serialized to JSON for IPC responses.
        """
        job_snapshots: List[Dict[str, Any]] = []
        if status_filter is None:
            filter_folded = None
        else:
            stripped = status_filter.strip()
            filter_folded = stripped.casefold() if stripped else None
        for job_id, job in self._jobs.items():
            status_data = job.get_status()
            status_value = status_data.get("status", JobStatus.PENDING)
            if isinstance(status_value, JobStatus):
                status_enum = status_value
            else:
                status_enum = JobStatus(int(status_value))
            status_text = public_status_name(status_enum)
            if filter_folded is not None:
                if status_text.casefold() != filter_folded:
                    continue
            created_at = self._job_creation_times.get(job_id, datetime.now())
            started_at = self._job_started_times.get(job_id)
            terminal_at = self._job_terminal_at.get(
                job_id
            ) or self._job_completed_times.get(job_id)
            snap: Dict[str, Any] = {
                "job_id": job_id,
                "job_type": self._job_types.get(job_id, ""),
                "status": status_text,
                "progress": int(status_data.get("progress", 0)),
                "description": status_data.get("description", ""),
                "result": status_data.get("result"),
                "is_running": job.is_running(),
                "created_at": created_at.isoformat(),
                "updated_at": datetime.now().isoformat(),
            }
            if started_at is not None:
                snap["started_at"] = started_at.isoformat()
            if terminal_at is not None:
                snap["completed_at"] = terminal_at.isoformat()
            job_snapshots.append(snap)
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
        current_status = normalize_public_job_status(status_data["status"])

        created_at = self._job_creation_times.get(job_id, datetime.now())
        started_at = self._job_started_times.get(job_id)
        completed_at = self._job_completed_times.get(job_id)

        # Update started_at if job is running but not tracked yet (raw RUNNING)
        raw_status = status_data["status"]
        if raw_status == JobStatus.RUNNING and started_at is None:
            started_at = datetime.now()
            self._job_started_times[job_id] = started_at

        if is_terminal_job_status(current_status):
            if job_id not in self._job_terminal_at:
                mark = datetime.now()
                self._job_terminal_at[job_id] = mark
            if completed_at is None:
                completed_at = self._job_terminal_at[job_id]
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
            """Hard-remove jobs during limit eviction (not soft user deletion)."""
            self._purge_job_immediately(target_job_id)

        if self.max_queue_size:
            self.cleanup_completed_jobs()
            while len(self._jobs) >= self.max_queue_size:
                if not self._evict_oldest_terminal_job():
                    break

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
        Mark a job as DELETED and retain it in memory until retention cleanup.

        If running, request STOP or terminate when force=True.

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
        created_at = self._job_creation_times.get(job_id, datetime.now())

        try:
            if job.is_running():
                if force:
                    job.terminate_process()
                else:
                    job.stop_process(timeout=10.0)  # 10 second timeout
        except ProcessControlError:
            if not force:
                raise
            try:
                job.terminate_process()
            except ProcessControlError:
                pass

        now = datetime.now()
        if job._shared_state is not None:
            try:
                update_job_state(job._shared_state, status=JobStatus.DELETED)
            except (ValueError, OSError, IOError, TypeError):
                logger.warning("Could not mark job %s DELETED in shared state", job_id)
        self._job_terminal_at[job_id] = now
        self._job_completed_times[job_id] = now

        progress_val = int(job.get_status().get("progress", 0))
        deletion_record = JobRecord(
            job_id=job_id,
            status=JobStatus.DELETED,
            progress=progress_val,
            description="Job deleted",
            result=None,
            created_at=created_at,
            updated_at=now,
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

        if is_terminal_job_status(current_status):
            raise InvalidJobStateError(job_id, current_status.name, "start")

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

    def _finalize_stop_authoritative(self, job_id: JobId, created_at: datetime) -> None:
        """
        Record STOPPED as the durable terminal outcome after a successful stop.

        Does not overwrite DELETED. Appends a registry snapshot and sets
        terminal retention timestamps.

        Args:
            job_id: Job identifier.
            created_at: Original job creation time for the snapshot.
        """
        job = self._jobs[job_id]
        raw = job.get_status()["status"]
        if raw == JobStatus.DELETED:
            return
        now = datetime.now()
        if job._shared_state is not None:
            update_job_state(
                job._shared_state,
                status=JobStatus.STOPPED,
                description=DESCRIPTION_JOB_STOPPED,
            )
        self._job_terminal_at[job_id] = now
        self._job_completed_times[job_id] = now
        snap = job.get_status()
        progress_val = int(snap.get("progress", 0))
        desc = snap.get("description") or DESCRIPTION_JOB_STOPPED
        started_at = self._job_started_times.get(job_id)
        record = JobRecord(
            job_id=job_id,
            status=JobStatus.STOPPED,
            progress=progress_val,
            description=str(desc) if desc is not None else DESCRIPTION_JOB_STOPPED,
            result=snap.get("result"),
            created_at=created_at,
            updated_at=now,
            started_at=started_at,
            completed_at=now,
        )
        self.registry.append(record)

    def stop_job(self, job_id: JobId, timeout: Optional[float] = None) -> None:
        """
        Request graceful STOP, wait up to timeout, and persist STOPPED.

        Running jobs receive STOP via ``stop_process``; the queue then forces
        authoritative STOPPED state (including over a late COMPLETED race).
        Pending or INTERRUPTED (not running) jobs are cancelled to STOPPED.
        Terminal jobs raise ``InvalidJobStateError``.

        Args:
            job_id: Job identifier to stop.
            timeout: Maximum time to wait for graceful stop (seconds).

        Raises:
            JobNotFoundError: If job is not found.
            InvalidJobStateError: If the job is already in a terminal state.
            ProcessControlError: If stop fails.
        """
        if job_id not in self._jobs:
            raise JobNotFoundError(job_id)

        job = self._jobs[job_id]
        created_at = self._job_creation_times.get(job_id, datetime.now())

        if job.is_running():
            try:
                job.stop_process(timeout=timeout)
            except ProcessControlError as e:
                raise ProcessControlError(job_id, "stop", e) from e
            self._finalize_stop_authoritative(job_id, created_at)
            return

        current = job.get_status()["status"]
        if current in (JobStatus.PENDING, JobStatus.INTERRUPTED):
            if job._shared_state is not None:
                update_job_state(
                    job._shared_state,
                    status=JobStatus.STOPPED,
                    description=DESCRIPTION_JOB_STOPPED,
                )
            self._finalize_stop_authoritative(job_id, created_at)
            return

        if current == JobStatus.RUNNING and not job.is_running():
            self._finalize_stop_authoritative(job_id, created_at)
            return

        if is_terminal_job_status(current):
            raise InvalidJobStateError(job_id, public_status_name(current), "stop")

        raise InvalidJobStateError(job_id, current.name, "stop")

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
        self._job_started_times.clear()
        self._job_completed_times.clear()
        self._job_terminal_at.clear()
        self._job_types.clear()
