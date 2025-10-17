"""
Example job that demonstrates how to work with job results.

This example shows how to:
1. Set a result in a job
2. Retrieve the result after job completion
3. Use results in different scenarios

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import time
import json

from queuemgr.proc_api import proc_queue_system
from queuemgr.jobs.base import QueueJobBase
from queuemgr.exceptions import ProcessControlError


class DataProcessingJob(QueueJobBase):
    """Job that processes data and returns a result."""

    def __init__(self, job_id: str, params: dict):
        """
        Initialize DataProcessingJob.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)
        self.data_size = params.get("data_size", 1000)

    def execute(self) -> None:
        """Process data and set result."""
        print(
            f"DataProcessingJob {self.job_id}: Processing {self.data_size} records..."
        )

        # Simulate data processing
        processed_data = []
        for i in range(self.data_size):
            if i % 100 == 0:
                print(
                    f"DataProcessingJob {self.job_id}: Processed {i}/{self.data_size}"
                )
            processed_data.append(f"record_{i}")
            time.sleep(0.001)  # Simulate work

        # Create result
        result = {
            "processed_records": len(processed_data),
            "data_size": self.data_size,
            "processing_time": time.time(),
            "sample_data": processed_data[:5],  # First 5
            "status": "completed",
        }

        # Set the result
        self.set_result(result)
        print(f"DataProcessingJob {self.job_id}: Processing completed, result set")

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


class CalculationJob(QueueJobBase):
    """Job that performs calculations and returns results."""

    def __init__(self, job_id: str, params: dict):
        """
        Initialize CalculationJob.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)
        self.operation = params.get("operation", "sum")
        self.numbers = params.get("numbers", [1, 2, 3, 4, 5])

    def execute(self) -> None:
        """Perform calculation and set result."""
        print(
            f"CalculationJob {self.job_id}: Performing {self.operation} "
            f"on {self.numbers}"
        )

        if self.operation == "sum":
            result = sum(self.numbers)
        elif self.operation == "product":
            result = 1
            for num in self.numbers:
                result *= num
        elif self.operation == "average":
            result = sum(self.numbers) / len(self.numbers)
        else:
            raise ValueError(f"Unknown operation: {self.operation}")

        # Set the result
        self.set_result(
            {
                "operation": self.operation,
                "numbers": self.numbers,
                "result": result,
                "timestamp": time.time(),
            }
        )

        print(f"CalculationJob {self.job_id}: Calculation completed, result: {result}")

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"CalculationJob {self.job_id}: Starting calculation...")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"CalculationJob {self.job_id}: Calculation stopped")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"CalculationJob {self.job_id}: Calculation completed successfully")

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        print(f"CalculationJob {self.job_id}: Error occurred: {exc}")


class FileAnalysisJob(QueueJobBase):
    """Job that analyzes files and returns analysis results."""

    def __init__(self, job_id: str, params: dict):
        """
        Initialize FileAnalysisJob.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)
        self.file_path = params.get("file_path", "/tmp/sample.txt")
        self.analysis_type = params.get("analysis_type", "basic")

    def execute(self) -> None:
        """Analyze file and set result."""
        print(f"FileAnalysisJob {self.job_id}: Analyzing {self.file_path}")

        # Simulate file analysis
        analysis_result = {
            "file_path": self.file_path,
            "analysis_type": self.analysis_type,
            "file_size": 1024 * 50,  # Simulated
            "line_count": 100,
            "word_count": 500,
            "char_count": 2500,
            "analysis_time": time.time(),
            "recommendations": [
                "File is well-structured",
                "Consider compression for storage",
                "Regular backup recommended",
            ],
        }

        # Set the result
        self.set_result(analysis_result)
        print(f"FileAnalysisJob {self.job_id}: Analysis completed")

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"FileAnalysisJob {self.job_id}: Starting file analysis...")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"FileAnalysisJob {self.job_id}: File analysis stopped")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"FileAnalysisJob {self.job_id}: File analysis completed successfully")

    def on_error(self, exc: BaseException) -> None:
        """Called when job encounters an error."""
        print(f"FileAnalysisJob {self.job_id}: Error occurred: {exc}")


def demonstrate_job_results():
    """Demonstrate how to work with job results."""
    print("🚀 Job Results Example")
    print("=" * 50)

    with proc_queue_system(
        registry_path="/tmp/result_example.jsonl", proc_dir="/tmp/result_example_proc"
    ) as queue:
        print("📋 Adding jobs with results...")

        # Add different types of jobs
        jobs = [
            (DataProcessingJob, "data-job-1", {"data_size": 500}),
            (
                CalculationJob,
                "calc-job-1",
                {"operation": "sum", "numbers": [1, 2, 3, 4, 5]},
            ),
            (
                CalculationJob,
                "calc-job-2",
                {"operation": "product", "numbers": [2, 3, 4]},
            ),
            (
                FileAnalysisJob,
                "file-job-1",
                {"file_path": "/tmp/document.txt", "analysis_type": "detailed"},
            ),
        ]

        for job_class, job_id, params in jobs:
            queue.add_job(job_class, job_id, params)
            print(f"➕ Added job: {job_id}")

        print("\n🚀 Starting jobs...")
        for _, job_id, _ in jobs:
            queue.start_job(job_id)
            print(f"▶️ Started job: {job_id}")

        print("\n⏳ Waiting for jobs to complete...")
        time.sleep(3)

        print("\n📊 Retrieving job results...")
        for _, job_id, _ in jobs:
            try:
                # Get job status (includes result)
                status = queue.get_job_status(job_id)
                print(f"\n📋 Job: {job_id}")
                print(f"   Status: {status.get('status', 'unknown')}")
                print(f"   Progress: {status.get('progress', 0)}%")
                print(f"   Description: {status.get('description', 'No description')}")

                # Get the result
                result = status.get("result")
                if result:
                    print(f"   Result: {json.dumps(result, indent=2)}")
                else:
                    print("   Result: No result available")

            except (
                OSError,
                IOError,
                ValueError,
                TimeoutError,
                ProcessControlError,
            ) as e:
                print(f"❌ Error getting status for {job_id}: {e}")

        print("\n✅ Job results demonstration completed!")


def demonstrate_result_usage():
    """Demonstrate different ways to use job results."""
    print("\n🎯 Result Usage Patterns")
    print("=" * 50)

    with proc_queue_system(
        registry_path="/tmp/usage_example.jsonl", proc_dir="/tmp/usage_example_proc"
    ) as queue:
        # Example 1: Data processing with result
        print("📊 Example 1: Data Processing")
        queue.add_job(DataProcessingJob, "data-example", {"data_size": 100})
        queue.start_job("data-example")
        time.sleep(2)

        status = queue.get_job_status("data-example")
        result = status.get("result")
        if result:
            print(f"   Processed {result['processed_records']} records")
            print(f"   Sample data: {result['sample_data']}")

        # Example 2: Calculation with result
        print("\n🧮 Example 2: Calculation")
        queue.add_job(
            CalculationJob,
            "calc-example",
            {"operation": "sum", "numbers": [10, 20, 30, 40, 50]},
        )
        queue.start_job("calc-example")
        time.sleep(1)

        status = queue.get_job_status("calc-example")
        result = status.get("result")
        if result:
            print(f"   Sum of {result['numbers']} = {result['result']}")

        # Example 3: File analysis with result
        print("\n📁 Example 3: File Analysis")
        queue.add_job(
            FileAnalysisJob,
            "file-example",
            {"file_path": "/tmp/important.txt", "analysis_type": "comprehensive"},
        )
        queue.start_job("file-example")
        time.sleep(1)

        status = queue.get_job_status("file-example")
        result = status.get("result")
        if result:
            print(f"   File: {result['file_path']}")
            print(f"   Size: {result['file_size']} bytes")
            print(f"   Lines: {result['line_count']}")
            print(f"   Recommendations: {len(result['recommendations'])} items")

        print("\n✅ Result usage demonstration completed!")


def main() -> None:
    """Main function demonstrating job results."""
    print("🎯 Queue Manager - Job Results Example")
    print("=" * 60)
    print("This example demonstrates how to work with job results:")
    print("1. Setting results in jobs")
    print("2. Retrieving results after completion")
    print("3. Using results in different scenarios")
    print()

    try:
        # Demonstrate basic job results
        demonstrate_job_results()

        # Demonstrate result usage patterns
        demonstrate_result_usage()

        print("\n🎉 All examples completed successfully!")
        print("\n📚 Key points:")
        print("- Use self.set_result() to store results in jobs")
        print("- Results are automatically saved when job completes")
        print("- Retrieve results via queue.get_job_status()")
        print("- Results can be any JSON-serializable data")

    except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
        print(f"❌ Error running examples: {e}")
        raise


if __name__ == "__main__":
    main()
