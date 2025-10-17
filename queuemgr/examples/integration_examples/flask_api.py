"""
Flask API implementation for Queue Manager.

This module contains the Flask API implementation for the integration example.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import time
from typing import Dict, Any, List

from queuemgr.proc_manager import ProcManager
from queuemgr.exceptions import ProcessControlError
from queuemgr.jobs.base import QueueJobBase


class QueueManagerWebAPI:
    """
    Web API for Queue Manager.

    Provides REST API endpoints for managing jobs and monitoring
    the queue system.
    """

    def __init__(self) -> None:
        """Initialize the web API."""
        self.queue = None

    def get_service_status(self) -> Dict[str, Any]:
        """Get service status."""
        try:
            if self.queue and self.queue.is_running():
                jobs = self.queue.list_jobs()
                running_jobs = [job for job in jobs if job.get("status") == "RUNNING"]

                return {
                    "status": "running",
                    "total_jobs": len(jobs),
                    "running_jobs": len(running_jobs),
                    "uptime": time.time(),  # Simplified
                }
            else:
                return {"status": "stopped"}

        except ProcessControlError:
            return {"status": "error", "message": "Service not available"}

    def start_service(self) -> Dict[str, Any]:
        """Start the queue service."""
        try:
            if not self.queue:
                self.queue = ProcManager()

            if not self.queue.is_running():
                self.queue.start()
                return {"status": "success", "message": "Service started"}
            else:
                return {"status": "success", "message": "Service already running"}

        except ProcessControlError as e:
            return {"status": "error", "message": f"Failed to start service: {e}"}

    def stop_service(self) -> Dict[str, Any]:
        """Stop the queue service."""
        try:
            if self.queue and self.queue.is_running():
                self.queue.stop()
                return {"status": "success", "message": "Service stopped"}
            else:
                return {"status": "success", "message": "Service not running"}

        except ProcessControlError as e:
            return {"status": "error", "message": f"Failed to stop service: {e}"}

    def get_jobs(self, status_filter: str | None = None) -> List[Dict[str, Any]]:
        """Get list of jobs with optional status filter."""
        try:
            if not self.queue or not self.queue.is_running():
                return []

            jobs = self.queue.list_jobs()

            if status_filter:
                jobs = [job for job in jobs if job.get("status") == status_filter]

            return jobs

        except ProcessControlError:
            return []

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get status of a specific job."""
        try:
            if not self.queue or not self.queue.is_running():
                return {"error": "Service not running"}

            return self.queue.get_job_status(job_id)

        except ProcessControlError as e:
            return {"error": f"Failed to get job status: {e}"}

    def start_job(self, job_id: str) -> Dict[str, Any]:
        """Start a job."""
        try:
            if not self.queue or not self.queue.is_running():
                return {"error": "Service not running"}

            self.queue.start_job(job_id)
            return {"status": "success", "message": f"Job {job_id} started"}

        except ProcessControlError as e:
            return {"error": f"Failed to start job: {e}"}

    def stop_job(self, job_id: str) -> Dict[str, Any]:
        """Stop a job."""
        try:
            if not self.queue or not self.queue.is_running():
                return {"error": "Service not running"}

            self.queue.stop_job(job_id)
            return {"status": "success", "message": f"Job {job_id} stopped"}

        except ProcessControlError as e:
            return {"error": f"Failed to stop job: {e}"}

    def delete_job(self, job_id: str) -> Dict[str, Any]:
        """Delete a job."""
        try:
            if not self.queue or not self.queue.is_running():
                return {"error": "Service not running"}

            self.queue.delete_job(job_id)
            return {"status": "success", "message": f"Job {job_id} deleted"}

        except ProcessControlError as e:
            return {"error": f"Failed to delete job: {e}"}

    def add_job(
        self, job_class_name: str, job_id: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Add a new job."""
        try:
            if not self.queue or not self.queue.is_running():
                return {"error": "Service not running"}

            # Import job class dynamically
            job_class = self._import_job_class(job_class_name)
            self.queue.add_job(job_class, job_id, params)
            return {"status": "success", "message": f"Job {job_id} added"}

        except ProcessControlError as e:
            return {"error": f"Failed to add job: {e}"}

    def _import_job_class(self, class_name: str) -> type:
        """Import job class dynamically."""
        # This is a simplified version - in real implementation,
        # you'd need proper module resolution
        if class_name == "SimpleJob":

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
        else:
            raise ValueError(f"Unknown job class: {class_name}")
