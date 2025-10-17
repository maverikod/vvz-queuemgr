"""
Progress reporting job example demonstrating long-running job with progress
updates.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import time
import uuid
from typing import Any, Dict

from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.types import JobStatus
from queuemgr.core.ipc import update_job_state
from queuemgr.queue.job_queue import JobQueue
from queuemgr.core.registry import JsonlRegistry


class ProgressJob(QueueJobBase):
    """
    A long-running job that reports progress periodically.
    """

    def __init__(self, job_id: str, params: Dict[str, Any]) -> None:
        """
        Initialize the progress job.

        Args:
            job_id: Unique job identifier.
            params: Job parameters containing 'duration' (seconds to run).
        """
        super().__init__(job_id, params)
        self.duration = params.get("duration", 10)  # seconds
        self.update_interval = params.get("update_interval", 0.5)  # seconds

    def execute(self) -> None:
        """Execute the long-running job with progress updates."""
        start_time = time.time()
        total_steps = int(self.duration / self.update_interval)

        for step in range(total_steps + 1):
            # Check if we should stop
            if not self._running:
                break

            # Calculate progress
            elapsed = time.time() - start_time
            progress = min(int((elapsed / self.duration) * 100), 100)

            # Update progress
            if self._shared_state is not None:
                update_job_state(
                    self._shared_state,
                    progress=progress,
                    description=f"Processing... {elapsed:.1f}s / {self.duration}s",
                )

            # Simulate work
            time.sleep(self.update_interval)

    def on_start(self) -> None:
        """Called when job starts."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state,
                description=f"Starting long-running job (duration: {self.duration}s)",
            )

    def on_stop(self) -> None:
        """Called when job is stopped."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state,
                description="Long-running job stopped by user",
            )

    def on_end(self) -> None:
        """Called when job completes successfully."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state,
                description="Long-running job completed successfully",
                result={"duration": self.duration, "completed": True},
            )

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state,
                description=f"Long-running job failed: {exc}",
                result={"error": str(exc)},
            )


def main():
    """Example usage of ProgressJob."""

    # Create registry and queue
    registry = JsonlRegistry("examples/progress_job_registry.jsonl")
    queue = JobQueue(registry)

    # Create and add job
    job_id = str(uuid.uuid4())
    job = ProgressJob(job_id, {"duration": 15, "update_interval": 0.5})
    queue.add_job(job)

    print(f"Created progress job with ID: {job_id}")

    # Start the job
    queue.start_job(job_id)
    print("Job started")

    # Monitor progress
    while True:
        status = queue.get_job_status(job_id)
        print(
            f"Status: {status.status.name}, Progress: {status.progress}%, Description: {status.description}"
        )

        if status.status in [
            JobStatus.COMPLETED,
            JobStatus.ERROR,
            JobStatus.INTERRUPTED,
        ]:
            break

        time.sleep(1.0)  # Check every second

    print(f"Job finished with status: {status.status.name}")
    if status.result:
        print(f"Result: {status.result}")


if __name__ == "__main__":
    main()
