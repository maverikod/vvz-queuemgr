# MCP Proxy Adapter Integration

This document describes how to integrate queuemgr with the MCP Proxy Adapter framework for creating microservices with job queue capabilities.

## Overview

The MCP Proxy Adapter integration allows you to create microservices that can manage job queues through HTTP API endpoints. This is particularly useful for building distributed systems where different services need to coordinate job execution.

## Components

### AsyncSimpleQueue

A simplified asyncio-compatible queue system that works directly in the current process without separate processes. This makes it suitable for asyncio applications like MCP Proxy Adapter.

```python
from queuemgr.async_simple_queue import AsyncSimpleQueue

# Create queue system
queue = AsyncSimpleQueue(registry_path="my_registry.jsonl")
await queue.start()

# Add and start jobs
await queue.add_job(MyJob, "job1", {"param": "value"})
await queue.start_job("job1")
```

### MCP Commands

Commands that integrate with the MCP Proxy Adapter framework to provide queue management functionality through HTTP API.

## Example Usage

### Basic Setup

```python
import asyncio
from mcp_proxy_adapter.commands.base import Command
from mcp_proxy_adapter.api.app import create_app
from queuemgr.async_simple_queue import AsyncSimpleQueue

# Global queue system
queue_system: AsyncSimpleQueue = None

class QueueCommand(Command):
    def __init__(self):
        super().__init__()
        self.name = "queue"
        self.description = "Queue management command"
        self.version = "1.0.0"
    
    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["add_job", "start_job", "stop_job", "get_status", "list_jobs"]
                },
                "job_id": {"type": "string"},
                "params": {"type": "object"}
            },
            "required": ["action"]
        }
    
    async def execute(self, params: dict) -> dict:
        action = params.get("action")
        job_id = params.get("job_id", "default_job")
        
        if action == "add_job":
            await queue_system.add_job(MyJob, job_id, params.get("params", {}))
            return {"message": f"Job {job_id} added successfully"}
        
        elif action == "start_job":
            await queue_system.start_job(job_id)
            return {"message": f"Job {job_id} started successfully"}
        
        # ... other actions
```

### Application Setup

```python
def create_mcp_app():
    app = create_app()
    
    # Register command
    from mcp_proxy_adapter.commands.command_registry import registry
    registry.register(QueueCommand())
    
    # Setup startup and cleanup
    @app.on_event("startup")
    async def startup_event():
        global queue_system
        queue_system = AsyncSimpleQueue(registry_path="/tmp/mcp_registry.jsonl")
        await queue_system.start()
    
    @app.on_event("shutdown")
    async def shutdown_event():
        global queue_system
        if queue_system:
            await queue_system.stop()
    
    return app
```

### Running the Server

```python
async def main():
    app = create_mcp_app()
    
    import uvicorn
    config = uvicorn.Config(
        app=app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
```

## API Usage

Once the server is running, you can interact with the queue system through HTTP API calls:

### Add a Job

```bash
curl -X POST http://localhost:8000/api/v1/execute \
  -H 'Content-Type: application/json' \
  -d '{
    "command": "queue",
    "params": {
      "action": "add_job",
      "job_id": "my_job",
      "params": {
        "task_name": "Process Data",
        "duration": 10
      }
    }
  }'
```

### Start a Job

```bash
curl -X POST http://localhost:8000/api/v1/execute \
  -H 'Content-Type: application/json' \
  -d '{
    "command": "queue",
    "params": {
      "action": "start_job",
      "job_id": "my_job"
    }
  }'
```

### Get Job Status

```bash
curl -X POST http://localhost:8000/api/v1/execute \
  -H 'Content-Type: application/json' \
  -d '{
    "command": "queue",
    "params": {
      "action": "get_status",
      "job_id": "my_job"
    }
  }'
```

### List All Jobs

```bash
curl -X POST http://localhost:8000/api/v1/execute \
  -H 'Content-Type: application/json' \
  -d '{
    "command": "queue",
    "params": {
      "action": "list_jobs"
    }
  }'
```

### Stop a Job

```bash
curl -X POST http://localhost:8000/api/v1/execute \
  -H 'Content-Type: application/json' \
  -d '{
    "command": "queue",
    "params": {
      "action": "stop_job",
      "job_id": "my_job"
    }
  }'
```

### Delete a Job

```bash
curl -X POST http://localhost:8000/api/v1/execute \
  -H 'Content-Type: application/json' \
  -d '{
    "command": "queue",
    "params": {
      "action": "delete_job",
      "job_id": "my_job",
      "force": true
    }
  }'
```

### Check Queue Health

```bash
curl -X POST http://localhost:8000/api/v1/execute \
  -H 'Content-Type: application/json' \
  -d '{
    "command": "queue",
    "params": {
      "action": "health"
    }
  }'
```

## Job Implementation

Jobs should inherit from `QueueJobBase` and implement the `execute` method:

```python
from queuemgr.jobs.base import QueueJobBase

class MyJob(QueueJobBase):
    def __init__(self, job_id: str, params: dict):
        super().__init__(job_id, params)
        self.task_name = params.get('task_name', 'default_task')
        self.duration = params.get('duration', 5)
    
    def execute(self) -> None:
        import time
        
        print(f"Starting task: {self.task_name}")
        
        for i in range(self.duration):
            time.sleep(1)
            progress = (i + 1) / self.duration * 100
            print(f"Progress: {progress:.1f}%")
        
        result = {
            "job_id": self.job_id,
            "task_name": self.task_name,
            "completed_at": time.time(),
            "status": "completed"
        }
        
        self.set_result(result)
```

## Examples

See the following example files:

- `queuemgr/examples/simple_mcp_example.py` - Simple MCP integration example
- `queuemgr/examples/working_mcp_server.py` - Complete working MCP server
- `queuemgr/examples/mcp_adapter_example.py` - Full-featured MCP integration

## Benefits

1. **Asyncio Compatibility**: Works seamlessly with asyncio-based applications
2. **HTTP API**: Provides RESTful interface for queue management
3. **Microservice Architecture**: Enables distributed job processing
4. **Easy Integration**: Simple to integrate with existing MCP Proxy Adapter applications
5. **No Separate Processes**: Runs in the same process as the web server

## Limitations

1. **Single Process**: Jobs run in the same process as the web server
2. **No Persistence**: Jobs are not persisted across server restarts
3. **Limited Concurrency**: Maximum concurrent jobs is limited by the server process

## Best Practices

1. **Error Handling**: Always handle exceptions in job execution
2. **Resource Management**: Monitor memory usage for long-running jobs
3. **Job Cleanup**: Regularly clean up completed jobs
4. **Monitoring**: Implement health checks and monitoring
5. **Logging**: Use proper logging for debugging and monitoring

## Troubleshooting

### Common Issues

1. **Queue System Not Running**: Ensure the queue system is properly initialized
2. **Job Not Found**: Check that the job ID exists before operations
3. **Concurrent Jobs Limit**: Monitor the number of running jobs
4. **Memory Usage**: Watch for memory leaks in long-running jobs

### Debugging

Enable debug logging to see detailed information about queue operations:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

This will show detailed information about job creation, execution, and completion.
