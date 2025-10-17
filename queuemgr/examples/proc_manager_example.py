"""
Example of using the ProcManager with /proc filesystem.

This example demonstrates how to use the ProcManager to manage
jobs using Linux /proc filesystem for inter-process communication.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import time
from queuemgr.proc_api import (
    proc_queue_system,
    add_job,
    start_job,
    get_job_status,
    list_jobs,
    start_proc_queue_system,
    stop_proc_queue_system,
)
from queuemgr.jobs.base import QueueJobBase


class LinuxJob(QueueJobBase):
    """Linux-optimized job for demonstration."""

    def __init__(self, job_id: str, params: dict):
        """
        Initialize LinuxJob.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)
        self.work_duration = params.get("duration", 5)
        self.process_name = params.get("process_name", f"LinuxJob-{job_id}")

    def execute(self) -> None:
        """Execute the job work."""
        print(
            f"Linux Job {self.job_id} starting work for {self.work_duration} seconds..."
        )
        print(f"Process name: {self.process_name}")

        for i in range(self.work_duration):
            time.sleep(1)
            print(f"Linux Job {self.job_id} working... {i+1}/{self.work_duration}")

        print(f"Linux Job {self.job_id} completed!")

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"Linux Job {self.job_id} started")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"Linux Job {self.job_id} stopped")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"Linux Job {self.job_id} ended")

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        print(f"Linux Job {self.job_id} error: {exc}")


def main() -> None:
    """Main function demonstrating ProcManager usage."""
    print("=== ProcManager Example (Linux /proc filesystem) ===")

    # Method 1: Using context manager (recommended)
    print("\n1. Using context manager with /proc:")
    with proc_queue_system(
        registry_path="linux_registry.jsonl", proc_dir="/tmp/queuemgr_linux"
    ) as queue:
        # Add some jobs
        queue.add_job(
            LinuxJob, "linux-job-1", {"duration": 3, "process_name": "Worker-1"}
        )
        queue.add_job(
            LinuxJob, "linux-job-2", {"duration": 5, "process_name": "Worker-2"}
        )
        queue.add_job(
            LinuxJob, "linux-job-3", {"duration": 2, "process_name": "Worker-3"}
        )

        # Start jobs
        queue.start_job("linux-job-1")
        queue.start_job("linux-job-2")

        # Check status
        print(f"Linux Job 1 status: {queue.get_job_status('linux-job-1')}")
        print(f"Linux Job 2 status: {queue.get_job_status('linux-job-2')}")

        # List all jobs
        jobs = queue.list_jobs()
        print(f"All Linux jobs: {len(jobs)}")

        # Wait a bit
        time.sleep(2)

        # Stop one job
        queue.stop_job("linux-job-1")

        # Wait for completion
        time.sleep(3)

    print("Context manager automatically stopped the system")

    # Method 2: Using global functions
    print("\n2. Using global functions with /proc:")

    start_proc_queue_system(
        registry_path="linux_registry2.jsonl", proc_dir="/tmp/queuemgr_linux2"
    )

    try:
        # Add and start jobs using global functions
        add_job(
            LinuxJob, "global-linux-job-1", {"duration": 2, "process_name": "Global-1"}
        )
        add_job(
            LinuxJob, "global-linux-job-2", {"duration": 3, "process_name": "Global-2"}
        )

        start_job("global-linux-job-1")
        start_job("global-linux-job-2")

        # Check status
        print(f"Global Linux job 1 status: {get_job_status('global-linux-job-1')}")
        print(f"Global Linux job 2 status: {get_job_status('global-linux-job-2')}")

        # List jobs
        jobs = list_jobs()
        print(f"Global Linux jobs: {len(jobs)}")

        # Wait for completion
        time.sleep(4)

    finally:
        stop_proc_queue_system()
        print("Global Linux system stopped")

    print("\n=== Linux /proc Example completed ===")


if __name__ == "__main__":
    main()
