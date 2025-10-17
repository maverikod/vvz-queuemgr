"""
Simple example of using the ProcessManager.

This example demonstrates how to use the ProcessManager to manage
jobs with minimal user intervention.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import time
from queuemgr.simple_api import (
    queue_system,
    add_job,
    start_job,
    get_job_status,
    list_jobs,
    start_queue_system,
    stop_queue_system,
)
from queuemgr.jobs.base import QueueJobBase


class SimpleJob(QueueJobBase):
    """Simple job for demonstration."""

    def __init__(self, job_id: str, params: dict):
        """
        Initialize SimpleJob.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)
        self.work_duration = params.get("duration", 5)

    def execute(self) -> None:
        """Execute the job work."""
        print(f"Job {self.job_id} starting work for {self.work_duration} seconds...")

        for i in range(self.work_duration):
            time.sleep(1)
            print(f"Job {self.job_id} working... {i+1}/{self.work_duration}")

        print(f"Job {self.job_id} completed!")

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"Job {self.job_id} started")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"Job {self.job_id} stopped")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"Job {self.job_id} ended")

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        print(f"Job {self.job_id} error: {exc}")


def main() -> None:
    """Main function demonstrating ProcessManager usage."""
    print("=== ProcessManager Example ===")

    # Method 1: Using context manager (recommended)
    print("\n1. Using context manager:")
    with queue_system(registry_path="example_registry.jsonl") as queue:
        # Add some jobs
        queue.add_job(SimpleJob, "job-1", {"duration": 3})
        queue.add_job(SimpleJob, "job-2", {"duration": 5})
        queue.add_job(SimpleJob, "job-3", {"duration": 2})

        # Start jobs
        queue.start_job("job-1")
        queue.start_job("job-2")

        # Check status
        print(f"Job 1 status: {queue.get_job_status('job-1')}")
        print(f"Job 2 status: {queue.get_job_status('job-2')}")

        # List all jobs
        jobs = queue.list_jobs()
        print(f"All jobs: {len(jobs)}")

        # Wait a bit
        time.sleep(2)

        # Stop one job
        queue.stop_job("job-1")

        # Wait for completion
        time.sleep(3)

    print("Context manager automatically stopped the system")

    # Method 2: Using global functions
    print("\n2. Using global functions:")

    start_queue_system(registry_path="example_registry2.jsonl")

    try:
        # Add and start jobs using global functions
        add_job(SimpleJob, "global-job-1", {"duration": 2})
        add_job(SimpleJob, "global-job-2", {"duration": 3})

        start_job("global-job-1")
        start_job("global-job-2")

        # Check status
        print(f"Global job 1 status: {get_job_status('global-job-1')}")
        print(f"Global job 2 status: {get_job_status('global-job-2')}")

        # List jobs
        jobs = list_jobs()
        print(f"Global jobs: {len(jobs)}")

        # Wait for completion
        time.sleep(4)

    finally:
        stop_queue_system()
        print("Global system stopped")

    print("\n=== Example completed ===")


if __name__ == "__main__":
    main()
