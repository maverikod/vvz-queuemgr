"""
Simple job example demonstrating basic job creation and execution.

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


class SimpleJob(QueueJobBase):
    """
    A simple job that counts from 1 to a specified number.
    """

    def __init__(self, job_id: str, params: Dict[str, Any]) -> None:
        """
        Initialize the simple job.

        Args:
            job_id: Unique job identifier.
            params: Job parameters containing 'count' (number to count to).
        """
        super().__init__(job_id, params)
        self.count = params.get("count", 10)

    def execute(self) -> None:
        """Execute the counting job."""
        for i in range(1, self.count + 1):
            # Check if we should stop
            if not self._running:
                break

            # Update progress
            progress = int((i / self.count) * 100)
            if self._shared_state is not None:
                update_job_state(
                    self._shared_state,
                    progress=progress,
                    description=f"Counting: {i}/{self.count}",
                )

            # Simulate work
            time.sleep(0.1)

    def on_start(self) -> None:
        """Called when job starts."""
        if self._shared_state is not None:
            if self._shared_state is not None:
                update_job_state(self._shared_state, description="Starting count job")

    def on_stop(self) -> None:
        """Called when job is stopped."""
        if self._shared_state is not None:
            if self._shared_state is not None:
                update_job_state(self._shared_state, description="Count job stopped")

    def on_end(self) -> None:
        """Called when job completes successfully."""
        if self._shared_state is not None:
            if self._shared_state is not None:
                update_job_state(
                    self._shared_state,
                    description=f"Count job completed: counted to {self.count}",
                    result={"final_count": self.count},
                )

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state,
                description=f"Count job failed: {exc}",
            )


def main():
    """Example usage of SimpleJob."""

    # Create registry and queue
    registry = JsonlRegistry("examples/simple_job_registry.jsonl")
    queue = JobQueue(registry)

    # Create and add job
    job_id = str(uuid.uuid4())
    job = SimpleJob(job_id, {"n": 20})
    queue.add_job(job)

    print(f"Created job with ID: {job_id}")

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

        time.sleep(0.5)

    print(f"Job finished with status: {status.status.name}")
    if status.result:
        print(f"Result: {status.result}")


if __name__ == "__main__":
    main()
