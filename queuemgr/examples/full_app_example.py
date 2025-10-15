"""
Full application example with real-world jobs.

This example demonstrates a complete application using the queue system
with various types of jobs: data processing, file operations, API calls, etc.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import json
import time
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from queuemgr.proc_api import proc_queue_system
from queuemgr.jobs.base import QueueJobBase


class DataProcessingJob(QueueJobBase):
    """Job for processing large datasets."""

    def __init__(self, job_id: str, params: dict):
        super().__init__(job_id, params)
        self.data_size = params.get("data_size", 1000)
        self.batch_size = params.get("batch_size", 100)

    def execute(self) -> None:
        """Process data in batches."""
        print(
            f"DataProcessingJob {self.job_id}: Processing {self.data_size} records..."
        )

        processed = 0
        for batch in range(0, self.data_size, self.batch_size):
            # Simulate data processing
            time.sleep(0.1)
            processed += min(self.batch_size, self.data_size - batch)

            # Update progress
            progress = int((processed / self.data_size) * 100)
            print(
                f"DataProcessingJob {self.job_id}: Processed {processed}/{self.data_size} ({progress}%)"
            )

        print(
            f"DataProcessingJob {self.job_id}: Completed processing {self.data_size} records"
        )

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"DataProcessingJob {self.job_id}: Starting data processing...")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"DataProcessingJob {self.job_id}: Data processing stopped")

    def on_end(self) -> None:
        """Called when job ends."""
        print(
            f"DataProcessingJob {self.job_id}: Data processing completed successfully"
        )

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        print(f"DataProcessingJob {self.job_id}: Error occurred: {exc}")


class FileOperationJob(QueueJobBase):
    """Job for file operations (copy, move, delete, etc.)."""

    def __init__(self, job_id: str, params: dict):
        super().__init__(job_id, params)
        self.operation = params.get("operation", "copy")
        self.source_path = params.get("source_path")
        self.dest_path = params.get("dest_path")
        self.file_size = params.get("file_size", 1024 * 1024)  # 1MB default

    def execute(self) -> None:
        """Perform file operation."""
        print(
            f"FileOperationJob {self.job_id}: {self.operation} from {self.source_path} to {self.dest_path}"
        )

        if self.operation == "copy":
            self._copy_file()
        elif self.operation == "move":
            self._move_file()
        elif self.operation == "delete":
            self._delete_file()
        elif self.operation == "create":
            self._create_file()
        else:
            raise ValueError(f"Unknown operation: {self.operation}")

    def _copy_file(self) -> None:
        """Copy file with progress."""
        if not self.source_path or not self.dest_path:
            raise ValueError("Source and destination paths required for copy")

        # Create destination directory if needed
        Path(self.dest_path).parent.mkdir(parents=True, exist_ok=True)

        # Simulate file copy with progress
        chunk_size = 1024 * 64  # 64KB chunks
        total_chunks = self.file_size // chunk_size

        for i in range(total_chunks):
            time.sleep(0.01)  # Simulate I/O
            progress = int((i / total_chunks) * 100)
            print(f"FileOperationJob {self.job_id}: Copying... {progress}%")

        print(f"FileOperationJob {self.job_id}: File copied successfully")

    def _move_file(self) -> None:
        """Move file."""
        if not self.source_path or not self.dest_path:
            raise ValueError("Source and destination paths required for move")

        # Simulate file move
        time.sleep(0.5)
        print(f"FileOperationJob {self.job_id}: File moved successfully")

    def _delete_file(self) -> None:
        """Delete file."""
        if not self.source_path:
            raise ValueError("Source path required for delete")

        # Simulate file deletion
        time.sleep(0.2)
        print(f"FileOperationJob {self.job_id}: File deleted successfully")

    def _create_file(self) -> None:
        """Create file."""
        if not self.dest_path:
            raise ValueError("Destination path required for create")

        # Create directory if needed
        Path(self.dest_path).parent.mkdir(parents=True, exist_ok=True)

        # Simulate file creation
        time.sleep(0.3)
        print(f"FileOperationJob {self.job_id}: File created successfully")

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"FileOperationJob {self.job_id}: Starting {self.operation} operation...")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"FileOperationJob {self.job_id}: File operation stopped")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"FileOperationJob {self.job_id}: File operation completed successfully")

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        print(f"FileOperationJob {self.job_id}: Error occurred: {exc}")


class APICallJob(QueueJobBase):
    """Job for making API calls."""

    def __init__(self, job_id: str, params: dict):
        super().__init__(job_id, params)
        self.url = params.get("url", "https://httpbin.org/delay/1")
        self.method = params.get("method", "GET")
        self.data = params.get("data", {})
        self.timeout = params.get("timeout", 30)

    def execute(self) -> None:
        """Make API call."""
        print(f"APICallJob {self.job_id}: Making {self.method} request to {self.url}")

        try:
            if self.method.upper() == "GET":
                response = requests.get(self.url, timeout=self.timeout)
            elif self.method.upper() == "POST":
                response = requests.post(self.url, json=self.data, timeout=self.timeout)
            elif self.method.upper() == "PUT":
                response = requests.put(self.url, json=self.data, timeout=self.timeout)
            elif self.method.upper() == "DELETE":
                response = requests.delete(self.url, timeout=self.timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {self.method}")

            response.raise_for_status()

            print(
                f"APICallJob {self.job_id}: API call successful (status: {response.status_code})"
            )

            # Store response data
            self._store_response(response)

        except requests.exceptions.RequestException as e:
            raise Exception(f"API call failed: {e}")

    def _store_response(self, response: requests.Response) -> None:
        """Store API response data."""
        response_data = {
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "content": response.text[:1000],  # Limit content size
            "timestamp": datetime.now().isoformat(),
        }

        # Save to file
        output_file = f"/tmp/api_response_{self.job_id}.json"
        with open(output_file, "w") as f:
            json.dump(response_data, f, indent=2)

        print(f"APICallJob {self.job_id}: Response saved to {output_file}")

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"APICallJob {self.job_id}: Starting API call...")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"APICallJob {self.job_id}: API call stopped")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"APICallJob {self.job_id}: API call completed successfully")

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        print(f"APICallJob {self.job_id}: Error occurred: {exc}")


class DatabaseJob(QueueJobBase):
    """Job for database operations."""

    def __init__(self, job_id: str, params: dict):
        super().__init__(job_id, params)
        self.operation = params.get("operation", "query")
        self.query = params.get("query", "SELECT * FROM users")
        self.records_count = params.get("records_count", 1000)

    def execute(self) -> None:
        """Perform database operation."""
        print(f"DatabaseJob {self.job_id}: Executing {self.operation} operation...")

        if self.operation == "query":
            self._execute_query()
        elif self.operation == "insert":
            self._execute_insert()
        elif self.operation == "update":
            self._execute_update()
        elif self.operation == "delete":
            self._execute_delete()
        else:
            raise ValueError(f"Unknown database operation: {self.operation}")

    def _execute_query(self) -> None:
        """Execute database query."""
        print(f"DatabaseJob {self.job_id}: Executing query: {self.query}")

        # Simulate query execution
        time.sleep(1.0)

        # Simulate processing results
        for i in range(0, self.records_count, 100):
            time.sleep(0.1)
            progress = int((i / self.records_count) * 100)
            print(f"DatabaseJob {self.job_id}: Processing results... {progress}%")

        print(f"DatabaseJob {self.job_id}: Query executed successfully")

    def _execute_insert(self) -> None:
        """Execute database insert."""
        print(f"DatabaseJob {self.job_id}: Inserting {self.records_count} records...")

        # Simulate batch insert
        batch_size = 100
        for i in range(0, self.records_count, batch_size):
            time.sleep(0.2)
            progress = int((i / self.records_count) * 100)
            print(f"DatabaseJob {self.job_id}: Inserting batch... {progress}%")

        print(f"DatabaseJob {self.job_id}: Insert completed successfully")

    def _execute_update(self) -> None:
        """Execute database update."""
        print(f"DatabaseJob {self.job_id}: Updating {self.records_count} records...")

        # Simulate update operation
        time.sleep(2.0)
        print(f"DatabaseJob {self.job_id}: Update completed successfully")

    def _execute_delete(self) -> None:
        """Execute database delete."""
        print(f"DatabaseJob {self.job_id}: Deleting {self.records_count} records...")

        # Simulate delete operation
        time.sleep(1.5)
        print(f"DatabaseJob {self.job_id}: Delete completed successfully")

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"DatabaseJob {self.job_id}: Starting database operation...")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"DatabaseJob {self.job_id}: Database operation stopped")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"DatabaseJob {self.job_id}: Database operation completed successfully")

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        print(f"DatabaseJob {self.job_id}: Error occurred: {exc}")


class MonitoringJob(QueueJobBase):
    """Job for system monitoring."""

    def __init__(self, job_id: str, params: dict):
        super().__init__(job_id, params)
        self.monitor_interval = params.get("monitor_interval", 5)
        self.monitor_duration = params.get("monitor_duration", 30)
        self.metrics: List[Dict[str, Any]] = []

    def execute(self) -> None:
        """Monitor system metrics."""
        print(
            f"MonitoringJob {self.job_id}: Starting system monitoring for {self.monitor_duration} seconds..."
        )

        start_time = time.time()
        while time.time() - start_time < self.monitor_duration:
            # Collect system metrics
            metrics = self._collect_metrics()
            self.metrics.append(metrics)

            print(
                f"MonitoringJob {self.job_id}: Collected metrics - CPU: {metrics['cpu']}%, Memory: {metrics['memory']}%"
            )

            time.sleep(self.monitor_interval)

        # Save metrics to file
        self._save_metrics()
        print(
            f"MonitoringJob {self.job_id}: Monitoring completed, collected {len(self.metrics)} data points"
        )

    def _collect_metrics(self) -> Dict[str, Any]:
        """Collect system metrics."""
        # Simulate metric collection
        import random

        return {
            "timestamp": datetime.now().isoformat(),
            "cpu": round(random.uniform(10, 90), 1),
            "memory": round(random.uniform(20, 80), 1),
            "disk": round(random.uniform(30, 70), 1),
            "network": round(random.uniform(5, 50), 1),
        }

    def _save_metrics(self) -> None:
        """Save collected metrics to file."""
        output_file = f"/tmp/metrics_{self.job_id}.json"
        with open(output_file, "w") as f:
            json.dump(self.metrics, f, indent=2)

        print(f"MonitoringJob {self.job_id}: Metrics saved to {output_file}")

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"MonitoringJob {self.job_id}: Starting system monitoring...")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"MonitoringJob {self.job_id}: System monitoring stopped")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"MonitoringJob {self.job_id}: System monitoring completed successfully")

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        print(f"MonitoringJob {self.job_id}: Error occurred: {exc}")


def main():
    """Main function demonstrating full application."""
    print("=== Full Application Example ===")
    print("This example demonstrates a complete application with various job types")
    print()

    # Create output directory
    output_dir = Path("/tmp/queuemgr_example")
    output_dir.mkdir(exist_ok=True)

    with proc_queue_system(
        registry_path=str(output_dir / "app_registry.jsonl"),
        proc_dir=str(output_dir / "proc"),
    ) as queue:
        print("🚀 Queue system started!")
        print()

        # 1. Data Processing Jobs
        print("📊 Adding data processing jobs...")
        queue.add_job(
            DataProcessingJob, "data-job-1", {"data_size": 5000, "batch_size": 200}
        )
        queue.add_job(
            DataProcessingJob, "data-job-2", {"data_size": 3000, "batch_size": 150}
        )

        # 2. File Operation Jobs
        print("📁 Adding file operation jobs...")
        queue.add_job(
            FileOperationJob,
            "file-job-1",
            {
                "operation": "create",
                "dest_path": str(output_dir / "sample_file.txt"),
                "file_size": 1024 * 1024,  # 1MB
            },
        )
        queue.add_job(
            FileOperationJob,
            "file-job-2",
            {
                "operation": "copy",
                "source_path": str(output_dir / "sample_file.txt"),
                "dest_path": str(output_dir / "copied_file.txt"),
                "file_size": 1024 * 1024,
            },
        )

        # 3. API Call Jobs
        print("🌐 Adding API call jobs...")
        queue.add_job(
            APICallJob,
            "api-job-1",
            {"url": "https://httpbin.org/delay/2", "method": "GET", "timeout": 10},
        )
        queue.add_job(
            APICallJob,
            "api-job-2",
            {
                "url": "https://httpbin.org/post",
                "method": "POST",
                "data": {"message": "Hello from QueueManager!"},
                "timeout": 10,
            },
        )

        # 4. Database Jobs
        print("🗄️ Adding database jobs...")
        queue.add_job(
            DatabaseJob,
            "db-job-1",
            {
                "operation": "query",
                "query": "SELECT * FROM users WHERE active = 1",
                "records_count": 2000,
            },
        )
        queue.add_job(
            DatabaseJob, "db-job-2", {"operation": "insert", "records_count": 1000}
        )

        # 5. Monitoring Jobs
        print("📈 Adding monitoring jobs...")
        queue.add_job(
            MonitoringJob,
            "monitor-job-1",
            {"monitor_interval": 3, "monitor_duration": 15},
        )

        print(f"✅ Added {len(queue.list_jobs())} jobs to the queue")
        print()

        # Start jobs in batches
        print("🚀 Starting jobs...")

        # Start data processing jobs
        queue.start_job("data-job-1")
        queue.start_job("data-job-2")

        # Wait a bit
        time.sleep(2)

        # Start file operations
        queue.start_job("file-job-1")
        queue.start_job("file-job-2")

        # Wait a bit
        time.sleep(2)

        # Start API calls
        queue.start_job("api-job-1")
        queue.start_job("api-job-2")

        # Wait a bit
        time.sleep(2)

        # Start database operations
        queue.start_job("db-job-1")
        queue.start_job("db-job-2")

        # Start monitoring
        queue.start_job("monitor-job-1")

        print("✅ All jobs started!")
        print()

        # Monitor job status
        print("📊 Monitoring job status...")
        for i in range(10):
            print(f"\n--- Status Check {i+1} ---")

            jobs = queue.list_jobs()
            for job in jobs:
                job_id = job.get("job_id", "unknown")
                status = job.get("status", "unknown")
                print(f"Job {job_id}: {status}")

            time.sleep(3)

        print("\n🛑 Stopping some jobs...")
        queue.stop_job("data-job-2")
        queue.stop_job("file-job-2")

        print("⏳ Waiting for remaining jobs to complete...")
        time.sleep(10)

        # Final status
        print("\n📋 Final job status:")
        jobs = queue.list_jobs()
        for job in jobs:
            job_id = job.get("job_id", "unknown")
            status = job.get("status", "unknown")
            print(f"Job {job_id}: {status}")

        print(f"\n📁 Output files created in: {output_dir}")
        print("📊 Check the generated files for results")

    print("\n✅ Application completed successfully!")
    print(
        "🎉 All jobs have been processed and the system has been shut down gracefully"
    )


if __name__ == "__main__":
    main()
