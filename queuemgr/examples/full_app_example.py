"""
Full application example with real-world jobs.

This example demonstrates a complete application using the queue system
with various types of jobs: data processing, file operations, API calls, etc.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import json
import time
import shutil
from pathlib import Path

from .proc_manager_example import proc_queue_system
from .jobs import DataProcessingJob, FileOperationJob, ApiCallJob
from queuemgr.exceptions import ProcessControlError


def demonstrate_full_application() -> None:
    """Demonstrate a full application with various job types."""
    print("🚀 Starting Full Application Example")
    print("=" * 50)

    try:
        # Start the queue system
        print("📋 Starting queue system...")
        proc_queue_system.start()
        print("✅ Queue system started")

        # Create test data
        test_data_dir = Path("test_data")
        test_data_dir.mkdir(exist_ok=True)

        # Create a test file
        test_file = test_data_dir / "test_file.txt"
        test_file.write_text("This is a test file for the full app example.")

        # Add various types of jobs
        jobs = []

        # 1. Data Processing Job
        print("\n📊 Adding Data Processing Job...")
        data_job_id = "data_processing_001"
        proc_queue_system.add_job(
            DataProcessingJob, data_job_id, {"data_size": 500, "batch_size": 50}
        )
        jobs.append(data_job_id)
        print(f"✅ Added data processing job: {data_job_id}")

        # 2. File Operation Job
        print("\n📁 Adding File Operation Job...")
        file_job_id = "file_operation_001"
        proc_queue_system.add_job(
            FileOperationJob,
            file_job_id,
            {
                "source_path": str(test_file),
                "destination_path": str(test_data_dir / "copied_file.txt"),
                "operation": "copy",
            },
        )
        jobs.append(file_job_id)
        print(f"✅ Added file operation job: {file_job_id}")

        # 3. API Call Job
        print("\n🌐 Adding API Call Job...")
        api_job_id = "api_call_001"
        proc_queue_system.add_job(
            ApiCallJob,
            api_job_id,
            {
                "url": "https://httpbin.org/json",
                "method": "GET",
                "headers": {"User-Agent": "QueueManager/1.0"},
            },
        )
        jobs.append(api_job_id)
        print(f"✅ Added API call job: {api_job_id}")

        # Start all jobs
        print("\n▶️ Starting all jobs...")
        for job_id in jobs:
            proc_queue_system.start_job(job_id)
            print(f"✅ Started job: {job_id}")

        # Monitor job progress
        print("\n📈 Monitoring job progress...")
        for _ in range(30):  # Monitor for 30 seconds
            time.sleep(1)

            all_completed = True
            for job_id in jobs:
                status = proc_queue_system.get_job_status(job_id)
                job_status = status.get("status", "unknown")
                progress = status.get("progress", 0)

                if job_status not in ["completed", "error", "stopped"]:
                    all_completed = False
                    print(f"📊 Job {job_id}: {job_status} ({progress}%)")

            if all_completed:
                break

        # Show final results
        print("\n📋 Final job status:")
        for job_id in jobs:
            status = proc_queue_system.get_job_status(job_id)
            job_status = status.get("status", "unknown")
            result = status.get("result")

            print(f"📊 Job {job_id}: {job_status}")
            if result:
                print(f"   Result: {json.dumps(result, indent=2)[:100]}...")

        # Cleanup
        print("\n🧹 Cleaning up...")
        for job_id in jobs:
            try:
                proc_queue_system.delete_job(job_id)
                print(f"✅ Deleted job: {job_id}")
            except (
                OSError,
                IOError,
                ValueError,
                TimeoutError,
                ProcessControlError,
            ) as e:
                print(f"⚠️ Failed to delete job {job_id}: {e}")

        # Clean up test data
        if test_data_dir.exists():
            shutil.rmtree(test_data_dir)
            print("✅ Cleaned up test data")

        print("\n🎉 Full application example completed successfully!")

    except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
        print(f"❌ Error in full application example: {e}")
        raise
    finally:
        # Stop the queue system
        try:
            print("\n🛑 Stopping queue system...")
            proc_queue_system.stop()
            print("✅ Queue system stopped")
        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            print(f"⚠️ Error stopping queue system: {e}")


def main() -> None:
    """Main function demonstrating full application."""
    demonstrate_full_application()


if __name__ == "__main__":
    main()
