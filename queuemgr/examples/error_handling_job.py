"""
Error handling job example demonstrating job failure and error reporting.

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


class ErrorHandlingJob(QueueJobBase):
    """
    A job that demonstrates error handling by intentionally failing.
    """

    def __init__(self, job_id: str, params: Dict[str, Any]) -> None:
        """
        Initialize the error handling job.

        Args:
            job_id: Unique job identifier.
            params: Job parameters containing 'fail_at' (step to fail at).
        """
        super().__init__(job_id, params)
        self.fail_at = params.get("fail_at", 5)
        self.steps = params.get("steps", 10)

    def execute(self) -> None:
        """Execute the job with intentional failure."""
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
                    description=f"Processing step {i}/{self.steps}",
                )

            # Intentionally fail at specified step
            if i == self.fail_at:
                raise ValueError(f"Intentional failure at step {i}")

            # Simulate work
            time.sleep(0.2)

    def on_start(self) -> None:
        """Called when job starts."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state, description="Starting error handling job"
            )

    def on_stop(self) -> None:
        """Called when job is stopped."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state, description="Error handling job stopped"
            )

    def on_end(self) -> None:
        """Called when job completes successfully."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state,
                description="Error handling job completed successfully",
                result={"completed_steps": self.steps},
            )

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        if self._shared_state is not None:
            update_job_state(
                self._shared_state,
                description=f"Error handling job failed: {exc}",
                result={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            )


def main():
    """Example usage of ErrorHandlingJob."""

    # Create registry and queue
    registry = JsonlRegistry("examples/error_handling_registry.jsonl")
    queue = JobQueue(registry)

    # Create and add job that will fail
    job_id = str(uuid.uuid4())
    job = ErrorHandlingJob(job_id, {"fail_at": 3, "steps": 10})
    queue.add_job(job)

    print(f"Created error handling job with ID: {job_id}")

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
