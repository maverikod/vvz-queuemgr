# AsyncIO Integration Guide

This guide explains how to use queuemgr with asyncio-based applications and web servers.

## Overview

queuemgr provides asyncio-compatible versions of its core components that work correctly with:
- aiohttp
- FastAPI
- Sanic
- Any asyncio-based application

## Key Components

### AsyncProcessManager

The asyncio-compatible version of ProcessManager that handles:
- AsyncIO-compatible signal handling
- Non-blocking queue operations
- Proper cleanup in asyncio contexts

### AsyncQueueSystem

The asyncio-compatible version of QueueSystem that provides:
- Async/await interface
- Context manager support
- Global queue system management

## Basic Usage

### Simple AsyncIO Application

```python
import asyncio
from queuemgr.async_simple_api import AsyncQueueSystem
from queuemgr.jobs.base import QueueJobBase

class MyJob(QueueJobBase):
    def run(self):
        # Your job logic here
        self.set_result({"status": "completed"})

async def main():
    # Create and start the queue system
    queue = AsyncQueueSystem(registry_path="/tmp/async_registry.jsonl")
    await queue.start()
    
    try:
        # Add and start a job
        await queue.add_job(MyJob, "job1", {})
        await queue.start_job("job1")
        
        # Get job status
        status = await queue.get_job_status("job1")
        print(f"Job status: {status}")
        
    finally:
        # Always cleanup
        await queue.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

### Using Context Manager

```python
import asyncio
from queuemgr.async_simple_api import async_queue_system_context
from queuemgr.jobs.base import QueueJobBase

class MyJob(QueueJobBase):
    def run(self):
        self.set_result({"status": "completed"})

async def main():
    async with async_queue_system_context() as queue:
        await queue.add_job(MyJob, "job1", {})
        await queue.start_job("job1")
        
        status = await queue.get_job_status("job1")
        print(f"Job status: {status}")
    # Queue system is automatically stopped

if __name__ == "__main__":
    asyncio.run(main())
```

## Web Server Integration

### aiohttp Example

```python
from aiohttp import web
from queuemgr.async_simple_api import AsyncQueueSystem

# Global queue system
queue_system = None

async def init_queue_system(app):
    global queue_system
    queue_system = AsyncQueueSystem()
    await queue_system.start()

async def cleanup_queue_system(app):
    global queue_system
    if queue_system:
        await queue_system.stop()

async def add_job_handler(request):
    data = await request.json()
    job_id = data.get("job_id")
    
    await queue_system.add_job(MyJob, job_id, data.get("params", {}))
    return web.json_response({"message": f"Job {job_id} added"})

app = web.Application()
app.on_startup.append(init_queue_system)
app.on_cleanup.append(cleanup_queue_system)
app.router.add_post("/jobs", add_job_handler)

if __name__ == "__main__":
    web.run_app(app)
```

### FastAPI Example

```python
from fastapi import FastAPI
from queuemgr.async_simple_api import AsyncQueueSystem

app = FastAPI()
queue_system = None

@app.on_event("startup")
async def startup():
    global queue_system
    queue_system = AsyncQueueSystem()
    await queue_system.start()

@app.on_event("shutdown")
async def shutdown():
    global queue_system
    if queue_system:
        await queue_system.stop()

@app.post("/jobs")
async def add_job(job_data: dict):
    await queue_system.add_job(MyJob, job_data["job_id"], job_data.get("params", {}))
    return {"message": "Job added"}
```

## Signal Handling

The asyncio-compatible version handles signals correctly:

```python
import signal
import asyncio
from queuemgr.async_simple_api import AsyncQueueSystem

async def signal_handler():
    """Handle shutdown signals gracefully."""
    print("Received shutdown signal, cleaning up...")
    # Queue system will be automatically cleaned up

# Register signal handlers
signal.signal(signal.SIGTERM, lambda s, f: asyncio.create_task(signal_handler()))
signal.signal(signal.SIGINT, lambda s, f: asyncio.create_task(signal_handler()))
```

## Error Handling

Always wrap queue operations in try-except blocks:

```python
import asyncio
from queuemgr.async_simple_api import AsyncQueueSystem
from queuemgr.exceptions import ProcessControlError

async def safe_queue_operation():
    queue = AsyncQueueSystem()
    
    try:
        await queue.start()
        await queue.add_job(MyJob, "job1", {})
        await queue.start_job("job1")
        
    except ProcessControlError as e:
        print(f"Queue operation failed: {e}")
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        
    finally:
        await queue.stop()
```

## Best Practices

### 1. Always Use Context Managers

```python
# ✅ Good
async with async_queue_system_context() as queue:
    await queue.add_job(MyJob, "job1", {})
    await queue.start_job("job1")

# ❌ Avoid
queue = AsyncQueueSystem()
await queue.start()
# ... operations ...
await queue.stop()  # Easy to forget
```

### 2. Handle Errors Gracefully

```python
async def robust_queue_operation():
    try:
        async with async_queue_system_context() as queue:
            await queue.add_job(MyJob, "job1", {})
            await queue.start_job("job1")
            
    except ProcessControlError as e:
        logger.error(f"Queue operation failed: {e}")
        # Handle the error appropriately
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        # Handle unexpected errors
```

### 3. Use Timeouts

```python
import asyncio

async def timeout_queue_operation():
    try:
        # Set a timeout for queue operations
        await asyncio.wait_for(
            queue.add_job(MyJob, "job1", {}),
            timeout=30.0
        )
    except asyncio.TimeoutError:
        print("Queue operation timed out")
```

### 4. Monitor Queue Health

```python
async def health_check():
    if queue_system and queue_system.is_running():
        return {"status": "healthy", "queue_running": True}
    else:
        return {"status": "unhealthy", "queue_running": False}
```

## Troubleshooting

### Common Issues

1. **Signal handling errors**: Use the asyncio-compatible version
2. **Queue timeout errors**: Use proper error handling and timeouts
3. **Process initialization failures**: Check registry path permissions

### Debug Mode

Enable debug logging:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("queuemgr")

# This will show detailed logs of queue operations
```

## Migration from Synchronous Version

If you're migrating from the synchronous version:

1. Replace `QueueSystem` with `AsyncQueueSystem`
2. Add `await` to all queue operations
3. Use `async with` for context managers
4. Handle signals properly in asyncio context

```python
# Old synchronous code
queue = QueueSystem()
queue.start()
queue.add_job(MyJob, "job1", {})
queue.stop()

# New asyncio code
async with async_queue_system_context() as queue:
    await queue.add_job(MyJob, "job1", {})
```

## Examples

See the following example files:
- `examples/async_web_example.py` - aiohttp integration
- `examples/async_fastapi_example.py` - FastAPI integration

## Support

For issues with asyncio integration, please:
1. Check the error logs
2. Verify signal handling setup
3. Ensure proper cleanup in your application
4. Test with the provided examples
