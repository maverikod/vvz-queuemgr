"""
CLI Integration Example for Queue Manager.

This example demonstrates how to create a command-line interface for Queue Manager.
Shows how to integrate job management into CLI applications.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import json
import time
from typing import Dict, Any

from queuemgr.proc_manager import ProcManager
from queuemgr.exceptions import ProcessControlError
from queuemgr.jobs.base import QueueJobBase


class QueueManagerCLI:
    """
    Command Line Interface for Queue Manager.

    Provides a comprehensive CLI for managing jobs and monitoring
    the queue system.
    """

    def __init__(self):
        """Initialize the CLI."""
        self.queue = None

    def start_service(
        self,
        registry_path: str = "/var/lib/queuemgr/registry.jsonl",
        proc_dir: str = "/var/run/queuemgr",
    ) -> None:
        """Start the queue service."""
        try:
            self.queue.start(registry_path, proc_dir)
            print("✅ Queue service started successfully")
        except ProcessControlError as e:
            print(f"❌ Failed to start service: {e}")

    def stop_service(self) -> None:
        """Stop the queue service."""
        try:
            self.queue.stop()
            print("✅ Queue service stopped successfully")
        except ProcessControlError as e:
            print(f"❌ Failed to stop service: {e}")

    def service_status(self) -> None:
        """Show service status."""
        try:
            queue = self.queue
            if queue.is_running():
                print("✅ Queue service is running")

                # Show basic stats
                jobs = queue.list_jobs()
                running_jobs = [job for job in jobs if job.get("status") == "RUNNING"]

                print(f"📊 Total jobs: {len(jobs)}")
                print(f"🏃 Running jobs: {len(running_jobs)}")

            else:
                print("❌ Queue service is not running")

        except ProcessControlError as e:
            print(f"❌ Service error: {e}")

    def add_job(self, job_class_name: str, job_id: str, params: Dict[str, Any]) -> None:
        """Add a job to the queue."""
        try:
            queue = self.queue

            # Import job class
            job_class = self._import_job_class(job_class_name)

            queue.add_job(job_class, job_id, params)
            print(f"✅ Job '{job_id}' added successfully")

        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            print(f"❌ Failed to add job: {e}")

    def start_job(self, job_id: str) -> None:
        """Start a job."""
        try:
            queue = self.queue
            queue.start_job(job_id)
            print(f"✅ Job '{job_id}' started successfully")

        except ProcessControlError as e:
            print(f"❌ Failed to start job: {e}")

    def stop_job(self, job_id: str) -> None:
        """Stop a job."""
        try:
            queue = self.queue
            queue.stop_job(job_id)
            print(f"✅ Job '{job_id}' stopped successfully")

        except ProcessControlError as e:
            print(f"❌ Failed to stop job: {e}")

    def delete_job(self, job_id: str, force: bool = False) -> None:
        """Delete a job."""
        try:
            queue = self.queue
            queue.delete_job(job_id, force)
            print(f"✅ Job '{job_id}' deleted successfully")

        except ProcessControlError as e:
            print(f"❌ Failed to delete job: {e}")

    def list_jobs(self, status_filter: str | None = None) -> None:
        """List all jobs."""
        try:
            queue = self.queue
            jobs = queue.list_jobs()

            if status_filter:
                jobs = [job for job in jobs if job.get("status") == status_filter]

            if not jobs:
                print("📭 No jobs found")
                return

            print(f"📋 Jobs ({len(jobs)}):")
            print("-" * 80)
            print(f"{'ID':<20} {'Status':<12} {'Progress':<8} {'Description':<30}")
            print("-" * 80)

            for job in jobs:
                job_id = job.get("job_id", "unknown")
                status = job.get("status", "unknown")
                progress = job.get("progress", 0)
                description = job.get("description", "")[:30]

                print(f"{job_id:<20} {status:<12} {progress:<8} {description:<30}")

        except ProcessControlError as e:
            print(f"❌ Failed to list jobs: {e}")

    def job_status(self, job_id: str) -> None:
        """Show detailed job status."""
        try:
            queue = self.queue
            status = queue.get_job_status(job_id)

            print(f"📊 Job Status: {job_id}")
            print("-" * 40)
            for key, value in status.items():
                print(f"{key}: {value}")

        except ProcessControlError as e:
            print(f"❌ Failed to get job status: {e}")

    def monitor_jobs(self, interval: int = 5) -> None:
        """Monitor jobs in real-time."""
        try:
            queue = self.queue

            print(f"🔍 Monitoring jobs (refresh every {interval}s, Ctrl+C to stop)...")
            print("-" * 80)

            while True:
                jobs = queue.list_jobs()
                running_jobs = [job for job in jobs if job.get("status") == "RUNNING"]

                print(
                    f"\r📊 Total: {len(jobs)}, Running: {len(running_jobs)}",
                    end="",
                    flush=True,
                )

                time.sleep(interval)

        except KeyboardInterrupt:
            print("\n✅ Monitoring stopped")
        except ProcessControlError as e:
            print(f"❌ Monitoring error: {e}")

    def _import_job_class(self, job_class_name: str):
        """Import job class by name."""
        # This is a simplified version - in practice, you'd need
        # a registry of available job classes

        # For demo purposes, return a simple job class
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

            def execute(self) -> None:
                """
                Execute the job.
                """
                print(f"SimpleJob {self.job_id} executed")

            def on_start(self) -> None:
                """
                Called when job starts.
                """
                print(f"SimpleJob {self.job_id} started")

            def on_stop(self) -> None:
                """
                Called when job stops.
                """
                print(f"SimpleJob {self.job_id} stopped")

            def on_end(self) -> None:
                """
                Called when job ends.
                """
                print(f"SimpleJob {self.job_id} ended")

            def on_error(self, exc: BaseException) -> None:
                """
                Called when job encounters an error.

                Args:
                    exc: The exception that occurred.
                """
                print(f"SimpleJob {self.job_id} error: {exc}")

        return SimpleJob


def main():
    """Main CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Queue Manager CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Service commands
    service_parser = subparsers.add_parser("service", help="Service management")
    service_subparsers = service_parser.add_subparsers(dest="service_command")

    service_subparsers.add_parser("start", help="Start the service")
    service_subparsers.add_parser("stop", help="Stop the service")
    service_subparsers.add_parser("status", help="Show service status")

    # Job commands
    job_parser = subparsers.add_parser("job", help="Job management")
    job_subparsers = job_parser.add_subparsers(dest="job_command")

    job_subparsers.add_parser("list", help="List all jobs")
    job_subparsers.add_parser("status", help="Show job status")
    job_subparsers.add_parser("start", help="Start a job")
    job_subparsers.add_parser("stop", help="Stop a job")
    job_subparsers.add_parser("delete", help="Delete a job")
    job_subparsers.add_parser("add", help="Add a job")

    # Monitor command
    subparsers.add_parser("monitor", help="Monitor jobs in real-time")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    cli = QueueManagerCLI()

    if args.command == "service":
        if args.service_command == "start":
            cli.start_service()
        elif args.service_command == "stop":
            cli.stop_service()
        elif args.service_command == "status":
            cli.service_status()
        else:
            service_parser.print_help()

    elif args.command == "job":
        if args.job_command == "list":
            cli.list_jobs()
        elif args.job_command == "status":
            job_id = input("Enter job ID: ")
            cli.job_status(job_id)
        elif args.job_command == "start":
            job_id = input("Enter job ID: ")
            cli.start_job(job_id)
        elif args.job_command == "stop":
            job_id = input("Enter job ID: ")
            cli.stop_job(job_id)
        elif args.job_command == "delete":
            job_id = input("Enter job ID: ")
            force = input("Force delete? (y/N): ").lower() == "y"
            cli.delete_job(job_id, force)
        elif args.job_command == "add":
            job_id = input("Enter job ID: ")
            params = input("Enter job parameters (JSON): ")
            try:
                params_dict = json.loads(params) if params else {}
            except json.JSONDecodeError:
                print("❌ Invalid JSON parameters")
                return
            cli.add_job("SimpleJob", job_id, params_dict)
        else:
            job_parser.print_help()

    elif args.command == "monitor":
        interval = int(input("Enter refresh interval (seconds, default 5): ") or "5")
        cli.monitor_jobs(interval)


if __name__ == "__main__":
    main()
