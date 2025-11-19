"""
AsyncIO web server example using queuemgr.

This example demonstrates how to use queuemgr with asyncio-based web servers
like aiohttp, FastAPI, or other async frameworks.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import asyncio
import json
from typing import Dict, Any
from aiohttp import web, ClientSession
from aiohttp.web import Request, Response

from queuemgr.async_simple_api import AsyncQueueSystem, async_queue_system_context
from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.types import JobStatus


class WebJob(QueueJobBase):
    """Example job for web processing."""

    def __init__(self, job_id: str, url: str, timeout: int = 30):
        """
        Initialize WebJob.

        Args:
            job_id: Unique job identifier.
            url: URL to process.
            timeout: Request timeout in seconds.
        """
        super().__init__(job_id)
        self.url = url
        self.timeout = timeout

    def run(self) -> None:
        """Execute the web job."""
        import aiohttp
        import asyncio

        async def fetch_url():
            async with ClientSession() as session:
                try:
                    async with session.get(self.url, timeout=self.timeout) as response:
                        data = await response.text()
                        return {
                            "status_code": response.status,
                            "content_length": len(data),
                            "headers": dict(response.headers),
                            "url": str(response.url),
                        }
                except Exception as e:
                    raise ValueError(f"Failed to fetch {self.url}: {e}")

        # Run the async function in the event loop
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(fetch_url())
        self.set_result(result)
        print(f"WebJob {self.job_id}: Successfully processed {self.url}")


class DataProcessingJob(QueueJobBase):
    """Example data processing job."""

    def __init__(self, job_id: str, data: Dict[str, Any]):
        """
        Initialize DataProcessingJob.

        Args:
            job_id: Unique job identifier.
            data: Data to process.
        """
        super().__init__(job_id)
        self.data = data

    def run(self) -> None:
        """Execute the data processing job."""
        # Simulate data processing
        import time

        time.sleep(2)  # Simulate processing time

        # Process the data
        processed_data = {
            "original_size": len(str(self.data)),
            "processed_at": time.time(),
            "data_keys": list(self.data.keys()) if isinstance(self.data, dict) else [],
            "processed": True,
        }

        self.set_result(processed_data)
        print(f"DataProcessingJob {self.job_id}: Processed data successfully")


# Global queue system
queue_system: AsyncQueueSystem = None


async def init_queue_system(app: web.Application) -> None:
    """Initialize the queue system."""
    global queue_system
    queue_system = AsyncQueueSystem(registry_path="/tmp/async_web_registry.jsonl")
    await queue_system.start()
    print("✅ Async queue system started")


async def cleanup_queue_system(app: web.Application) -> None:
    """Cleanup the queue system."""
    global queue_system
    if queue_system:
        await queue_system.stop()
        print("✅ Async queue system stopped")


async def add_job_handler(request: Request) -> Response:
    """Add a new job to the queue."""
    try:
        data = await request.json()
        job_type = data.get("job_type")
        job_id = data.get("job_id")
        params = data.get("params", {})

        if not job_type or not job_id:
            return web.json_response(
                {"error": "job_type and job_id are required"}, status=400
            )

        # Map job types to classes
        job_classes = {"web": WebJob, "data": DataProcessingJob}

        if job_type not in job_classes:
            return web.json_response(
                {"error": f"Unknown job type: {job_type}"}, status=400
            )

        await queue_system.add_job(job_classes[job_type], job_id, params)

        return web.json_response(
            {
                "message": f"Job {job_id} added successfully",
                "job_id": job_id,
                "job_type": job_type,
            }
        )

    except Exception as e:
        return web.json_response({"error": f"Failed to add job: {str(e)}"}, status=500)


async def start_job_handler(request: Request) -> Response:
    """Start a job."""
    try:
        data = await request.json()
        job_id = data.get("job_id")

        if not job_id:
            return web.json_response({"error": "job_id is required"}, status=400)

        await queue_system.start_job(job_id)

        return web.json_response(
            {"message": f"Job {job_id} started successfully", "job_id": job_id}
        )

    except Exception as e:
        return web.json_response(
            {"error": f"Failed to start job: {str(e)}"}, status=500
        )


async def stop_job_handler(request: Request) -> Response:
    """Stop a job."""
    try:
        data = await request.json()
        job_id = data.get("job_id")

        if not job_id:
            return web.json_response({"error": "job_id is required"}, status=400)

        await queue_system.stop_job(job_id)

        return web.json_response(
            {"message": f"Job {job_id} stopped successfully", "job_id": job_id}
        )

    except Exception as e:
        return web.json_response({"error": f"Failed to stop job: {str(e)}"}, status=500)


async def get_job_status_handler(request: Request) -> Response:
    """Get job status."""
    try:
        job_id = request.match_info.get("job_id")

        if not job_id:
            return web.json_response({"error": "job_id is required"}, status=400)

        status = await queue_system.get_job_status(job_id)

        return web.json_response({"job_id": job_id, "status": status})

    except Exception as e:
        return web.json_response(
            {"error": f"Failed to get job status: {str(e)}"}, status=500
        )


async def list_jobs_handler(request: Request) -> Response:
    """List all jobs."""
    try:
        jobs = await queue_system.list_jobs()

        return web.json_response({"jobs": jobs, "count": len(jobs)})

    except Exception as e:
        return web.json_response(
            {"error": f"Failed to list jobs: {str(e)}"}, status=500
        )


async def health_handler(request: Request) -> Response:
    """Health check endpoint."""
    try:
        is_running = queue_system.is_running() if queue_system else False

        return web.json_response(
            {
                "status": "healthy" if is_running else "unhealthy",
                "queue_running": is_running,
                "timestamp": asyncio.get_event_loop().time(),
            }
        )

    except Exception as e:
        return web.json_response(
            {"error": f"Health check failed: {str(e)}"}, status=500
        )


def create_app() -> web.Application:
    """Create the aiohttp application."""
    app = web.Application()

    # Setup startup and cleanup
    app.on_startup.append(init_queue_system)
    app.on_cleanup.append(cleanup_queue_system)

    # Add routes
    app.router.add_post("/jobs", add_job_handler)
    app.router.add_post("/jobs/{job_id}/start", start_job_handler)
    app.router.add_post("/jobs/{job_id}/stop", stop_job_handler)
    app.router.add_get("/jobs/{job_id}/status", get_job_status_handler)
    app.router.add_get("/jobs", list_jobs_handler)
    app.router.add_get("/health", health_handler)

    return app


async def main():
    """Main function to run the server."""
    print("🚀 Starting AsyncIO Web Server with queuemgr")

    app = create_app()

    # Run the server
    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "localhost", 8080)
    await site.start()

    print("✅ Server started at http://localhost:8080")
    print("📋 Available endpoints:")
    print("  POST /jobs - Add a job")
    print("  POST /jobs/{job_id}/start - Start a job")
    print("  POST /jobs/{job_id}/stop - Stop a job")
    print("  GET /jobs/{job_id}/status - Get job status")
    print("  GET /jobs - List all jobs")
    print("  GET /health - Health check")
    print()
    print("📝 Example usage:")
    print("  curl -X POST http://localhost:8080/jobs \\")
    print("    -H 'Content-Type: application/json' \\")
    print(
        '    -d \'{"job_type": "web", "job_id": "test1", "params": {"url": "https://httpbin.org/get"}}\''
    )
    print()

    try:
        # Keep the server running
        await asyncio.Future()  # Run forever
    except KeyboardInterrupt:
        print("\n🛑 Shutting down server...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
