"""
FastAPI example using queuemgr with asyncio.

This example demonstrates how to use queuemgr with FastAPI
and other async frameworks.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import asyncio
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

from queuemgr.async_simple_api import AsyncQueueSystem, async_queue_system_context
from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.types import JobStatus


# Pydantic models for API
class JobRequest(BaseModel):
    """
    Request payload describing a job to create via FastAPI endpoint.

    Attributes:
        job_type: Name of the job class to schedule.
        job_id: External identifier that must be unique within registry.
        params: Arbitrary job parameters passed to the job constructor.
    """

    job_type: str
    job_id: str
    params: Dict[str, Any] = {}


class JobResponse(BaseModel):
    """
    Response payload confirming operations performed on a job.

    Attributes:
        message: Human-readable description of the operation outcome.
        job_id: Identifier of the affected job.
        job_type: Optional job type associated with the response.
    """

    message: str
    job_id: str
    job_type: Optional[str] = None


class JobStatusResponse(BaseModel):
    """
    Response payload containing status details for a single job.

    Attributes:
        job_id: Identifier of the job being described.
        status: Serialized job status dictionary returned by queue system.
    """

    job_id: str
    status: Dict[str, Any]


class JobsListResponse(BaseModel):
    """
    Response payload listing all known jobs with total count.

    Attributes:
        jobs: Collection of serialized job entries from queue system.
        count: Total number of jobs returned in the response.
    """

    jobs: List[Dict[str, Any]]
    count: int


class HealthResponse(BaseModel):
    """
    Health-check response describing queue state and timestamp.

    Attributes:
        status: Textual indicator such as ``healthy`` or ``unhealthy``.
        queue_running: True when the queue manager is active.
        timestamp: Event loop timestamp corresponding to the health check.
    """

    status: str
    queue_running: bool
    timestamp: float


# Example job classes
class FastAPIJob(QueueJobBase):
    """Example job for FastAPI processing."""

    def __init__(self, job_id: str, data: Dict[str, Any]):
        """
        Initialize FastAPIJob.

        Args:
            job_id: Unique job identifier.
            data: Data to process.
        """
        super().__init__(job_id)
        self.data = data

    def run(self) -> None:
        """Execute the FastAPI job."""
        import time
        import json

        # Simulate processing
        time.sleep(1)

        # Process the data
        result = {
            "job_id": self.job_id,
            "processed_at": time.time(),
            "data_size": len(json.dumps(self.data)),
            "data_keys": list(self.data.keys()) if isinstance(self.data, dict) else [],
            "processed": True,
        }

        self.set_result(result)
        print(f"FastAPIJob {self.job_id}: Processed successfully")


class BackgroundTaskJob(QueueJobBase):
    """Example background task job."""

    def __init__(self, job_id: str, task_name: str, duration: int = 5):
        """
        Initialize BackgroundTaskJob.

        Args:
            job_id: Unique job identifier.
            task_name: Name of the task.
            duration: Task duration in seconds.
        """
        super().__init__(job_id)
        self.task_name = task_name
        self.duration = duration

    def run(self) -> None:
        """Execute the background task job."""
        import time

        print(f"BackgroundTaskJob {self.job_id}: Starting task '{self.task_name}'")

        # Simulate long-running task
        for i in range(self.duration):
            time.sleep(1)
            progress = (i + 1) / self.duration * 100
            self.update_progress(int(progress))
            print(f"BackgroundTaskJob {self.job_id}: Progress {progress:.1f}%")

        result = {
            "job_id": self.job_id,
            "task_name": self.task_name,
            "duration": self.duration,
            "completed_at": time.time(),
            "status": "completed",
        }

        self.set_result(result)
        print(f"BackgroundTaskJob {self.job_id}: Task completed")


# Global queue system
queue_system: Optional[AsyncQueueSystem] = None


# FastAPI app
app = FastAPI(
    title="queuemgr FastAPI Example",
    description="Example FastAPI application using queuemgr with asyncio",
    version="1.0.0",
)


@app.on_event("startup")
async def startup_event():
    """Initialize the queue system on startup."""
    global queue_system
    queue_system = AsyncQueueSystem(registry_path="/tmp/fastapi_registry.jsonl")
    await queue_system.start()
    print("✅ Async queue system started")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup the queue system on shutdown."""
    global queue_system
    if queue_system:
        await queue_system.stop()
        print("✅ Async queue system stopped")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    is_running = queue_system.is_running() if queue_system else False

    return HealthResponse(
        status="healthy" if is_running else "unhealthy",
        queue_running=is_running,
        timestamp=asyncio.get_event_loop().time(),
    )


@app.post("/jobs", response_model=JobResponse)
async def add_job(job_request: JobRequest):
    """Add a new job to the queue."""
    if not queue_system:
        raise HTTPException(status_code=500, detail="Queue system not initialized")

    # Map job types to classes
    job_classes = {"fastapi": FastAPIJob, "background": BackgroundTaskJob}

    if job_request.job_type not in job_classes:
        raise HTTPException(
            status_code=400, detail=f"Unknown job type: {job_request.job_type}"
        )

    try:
        await queue_system.add_job(
            job_classes[job_request.job_type], job_request.job_id, job_request.params
        )

        return JobResponse(
            message=f"Job {job_request.job_id} added successfully",
            job_id=job_request.job_id,
            job_type=job_request.job_type,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to add job: {str(e)}")


@app.post("/jobs/{job_id}/start", response_model=JobResponse)
async def start_job(job_id: str):
    """Start a job."""
    if not queue_system:
        raise HTTPException(status_code=500, detail="Queue system not initialized")

    try:
        await queue_system.start_job(job_id)

        return JobResponse(message=f"Job {job_id} started successfully", job_id=job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start job: {str(e)}")


@app.post("/jobs/{job_id}/stop", response_model=JobResponse)
async def stop_job(job_id: str):
    """Stop a job."""
    if not queue_system:
        raise HTTPException(status_code=500, detail="Queue system not initialized")

    try:
        await queue_system.stop_job(job_id)

        return JobResponse(message=f"Job {job_id} stopped successfully", job_id=job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop job: {str(e)}")


@app.get("/jobs/{job_id}/status", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get job status."""
    if not queue_system:
        raise HTTPException(status_code=500, detail="Queue system not initialized")

    try:
        status = await queue_system.get_job_status(job_id)

        return JobStatusResponse(job_id=job_id, status=status)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get job status: {str(e)}"
        )


@app.get("/jobs", response_model=JobsListResponse)
async def list_jobs():
    """List all jobs."""
    if not queue_system:
        raise HTTPException(status_code=500, detail="Queue system not initialized")

    try:
        jobs = await queue_system.list_jobs()

        return JobsListResponse(jobs=jobs, count=len(jobs))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")


@app.post("/jobs/{job_id}/delete")
async def delete_job(job_id: str, force: bool = False):
    """Delete a job."""
    if not queue_system:
        raise HTTPException(status_code=500, detail="Queue system not initialized")

    try:
        await queue_system.delete_job(job_id, force)

        return JobResponse(message=f"Job {job_id} deleted successfully", job_id=job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")


# Example of using the context manager
@app.post("/jobs/batch")
async def add_batch_jobs(jobs: List[JobRequest]):
    """Add multiple jobs using the context manager."""
    results = []

    async with async_queue_system_context(
        registry_path="/tmp/batch_registry.jsonl"
    ) as batch_queue:
        for job_request in jobs:
            try:
                job_classes = {"fastapi": FastAPIJob, "background": BackgroundTaskJob}

                if job_request.job_type not in job_classes:
                    results.append(
                        {
                            "job_id": job_request.job_id,
                            "status": "error",
                            "error": f"Unknown job type: {job_request.job_type}",
                        }
                    )
                    continue

                await batch_queue.add_job(
                    job_classes[job_request.job_type],
                    job_request.job_id,
                    job_request.params,
                )

                results.append({"job_id": job_request.job_id, "status": "added"})

            except Exception as e:
                results.append(
                    {"job_id": job_request.job_id, "status": "error", "error": str(e)}
                )

    return {"results": results}


if __name__ == "__main__":
    import uvicorn

    print("🚀 Starting FastAPI server with queuemgr")
    print("📋 Available endpoints:")
    print("  GET /health - Health check")
    print("  POST /jobs - Add a job")
    print("  POST /jobs/{job_id}/start - Start a job")
    print("  POST /jobs/{job_id}/stop - Stop a job")
    print("  GET /jobs/{job_id}/status - Get job status")
    print("  GET /jobs - List all jobs")
    print("  POST /jobs/{job_id}/delete - Delete a job")
    print("  POST /jobs/batch - Add multiple jobs")
    print()
    print("📝 Example usage:")
    print("  curl -X POST http://localhost:8000/jobs \\")
    print("    -H 'Content-Type: application/json' \\")
    print(
        '    -d \'{"job_type": "fastapi", "job_id": "test1", "params": {"data": {"key": "value"}}}\''
    )
    print()

    uvicorn.run(app, host="0.0.0.0", port=8000)
