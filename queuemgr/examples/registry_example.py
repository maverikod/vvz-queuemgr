"""
Registry example demonstrating job state persistence and retrieval.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import time
import uuid
from typing import Any, Dict

from ..queue.job_queue import JobQueue
from ..core.registry import JsonlRegistry
from ..core.types import JobStatus
from ..jobs.base import QueueJobBase
from ..core.ipc import update_job_state


class RegistryDemoJob(QueueJobBase):
    """
    A job that demonstrates registry functionality.
    """

    def __init__(self, job_id: str, params: Dict[str, Any]) -> None:
        """
        Initialize the registry demo job.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)
        self.steps = params.get("steps", 5)

    def execute(self) -> None:
        """Execute the registry demo job."""
        for i in range(1, self.steps + 1):
            # Check if we should stop
            if not self._running:
                break

            # Update progress
            progress = int((i / self.steps) * 100)
            if self._shared_state is not None:
                update_job_state(
                    self._shared_state,
                    progress=progress,
                    description=f"Registry demo step {i}/{self.steps}",
                )

            # Simulate work
            time.sleep(0.5)

    def on_start(self) -> None:
        """Called when job starts."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state, description="Starting registry demo job"
            )

    def on_stop(self) -> None:
        """Called when job is stopped."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state, description="Registry demo job stopped"
            )

    def on_end(self) -> None:
        """Called when job completes successfully."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state,
                description="Registry demo job completed",
                result={"steps_completed": self.steps},
            )

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state,
                description=f"Registry demo job failed: {exc}",
            )


def main():
    """Example usage demonstrating registry functionality."""
    # Create registry and queue
    registry = JsonlRegistry("examples/registry_demo.jsonl")
    queue = JobQueue(registry)

    print("=== Registry Demo ===")

    # Create multiple jobs
    job_ids = []
    for i in range(3):
        job_id = str(uuid.uuid4())
        job = RegistryDemoJob(job_id, {"steps": 3})
        queue.add_job(job)
        job_ids.append(job_id)
        print(f"Created job {i+1} with ID: {job_id}")

    # Start all jobs
    for job_id in job_ids:
        queue.start_job(job_id)
        print(f"Started job: {job_id}")

    # Monitor all jobs
    print("\nMonitoring jobs...")
    while True:
        all_completed = True
        for job_id in job_ids:
            status = queue.get_job_status(job_id)
            if status.status not in [
                JobStatus.COMPLETED,
                JobStatus.ERROR,
                JobStatus.INTERRUPTED,
            ]:
                all_completed = False
                break

        if all_completed:
            break

        time.sleep(1.0)

    print("\nAll jobs completed!")

    # Demonstrate registry functionality
    print("\n=== Registry Queries ===")

    # Get latest records for all jobs
    print("Latest records for all jobs:")
    for record in registry.all_latest():
        print(f"  Job {record.job_id}: {record.status.name} - {record.description}")

    # Get history for first job
    if job_ids:
        first_job_id = job_ids[0]
        print(f"\nHistory for job {first_job_id}:")
        history = registry.get_job_history(first_job_id)
        for i, record in enumerate(history):
            print(
                f"  Step {i+1}: {record.status.name} - {record.description} (Progress: {record.progress}%)"
            )

    # Demonstrate job status queries
    print("\nJob statuses:")
    statuses = queue.list_job_statuses()
    for job_id, status in statuses.items():
        print(f"  {job_id}: {status.name}")

    # Clean up
    print("\nCleaning up completed jobs...")
    removed_count = queue.cleanup_completed_jobs()
    print(f"Removed {removed_count} completed jobs")
    print(f"Remaining jobs: {queue.get_job_count()}")


if __name__ == "__main__":
    main()
