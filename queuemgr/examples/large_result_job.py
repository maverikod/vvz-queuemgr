"""
Example job with large result data (~1MB).

This example demonstrates how to work with large results in Queue Manager:
1. Generating large datasets
2. Storing results efficiently
3. Retrieving and processing large results
4. Memory management considerations

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import time

from queuemgr.proc_api import proc_queue_system
from .large_data_generator import LargeDataGeneratorJob
from .data_analyzer import DataAnalyzerJob
from queuemgr.exceptions import ProcessControlError


def demonstrate_large_results() -> None:
    """Demonstrate working with large results."""
    print("🚀 Demonstrating large result handling...")

    # Get queue system
    queue = proc_queue_system()

    try:
        # Start queue system
        print("📊 Starting queue system...")
        queue.start()

        # Add data generator job
        print("📝 Adding data generator job...")
        queue.add_job(
            LargeDataGeneratorJob,
            "large-data-gen-1",
            {"target_size_mb": 0.5, "data_type": "mixed"},  # 500KB for demo
        )

        # Add data analyzer job
        print("🔍 Adding data analyzer job...")
        queue.add_job(
            DataAnalyzerJob,
            "data-analyzer-1",
            {"analysis_type": "statistical", "sample_size": 1000},
        )

        # Start jobs
        print("▶️ Starting jobs...")
        queue.start_job("large-data-gen-1")
        queue.start_job("data-analyzer-1")

        # Wait for completion
        print("⏳ Waiting for jobs to complete...")
        time.sleep(10)

        # Check results
        print("📋 Checking results...")
        jobs = queue.list_jobs()

        for job in jobs:
            job_id = job.get("job_id")
            status = job.get("status")
            result = job.get("result")

            print(f"Job {job_id}: {status}")

            if result:
                if "data_type" in result:
                    print(
                        f"  📊 Generated {result['actual_size_mb']:.2f}MB of {result['data_type']} data"
                    )
                    print(f"  ⏱️ Generation time: {result['generation_time']:.2f}s")
                elif "analysis_type" in result:
                    print(f"  🔍 Analysis type: {result['analysis_type']}")
                    print(f"  ⏱️ Analysis time: {result['analysis_time']:.2f}s")

        print("✅ Large result demonstration completed!")

    except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
        print(f"❌ Error during demonstration: {e}")

    finally:
        # Cleanup
        print("🧹 Cleaning up...")
        try:
            queue.stop()
        except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
            print(f"⚠️ Error during cleanup: {e}")


def main() -> None:
    """Main function demonstrating large job results."""
    print("🎯 Large Result Job Example")
    print("=" * 50)

    try:
        demonstrate_large_results()
    except (OSError, IOError, ValueError, TimeoutError, ProcessControlError) as e:
        print(f"❌ Demonstration failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
