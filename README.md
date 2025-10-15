# Queue Manager

A Python-based job queue system with per-job processes and signal variables.

## Features

- **Per-Job Processes**: Each job runs in its own OS process for isolation
- **Signal Variables**: Jobs communicate via shared state protected by mutexes
- **Registry Persistence**: Job states and results are persisted to a registry
- **Process Control**: Start, stop, and delete jobs with graceful termination
- **Progress Tracking**: Real-time progress updates and status monitoring
- **Error Handling**: Comprehensive error handling and recovery mechanisms

## Installation

```bash
pip install queuemgr
```

## Quick Start

```python
from queuemgr.queue.job_queue import JobQueue
from queuemgr.core.registry import JsonlRegistry
from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.types import JobStatus
from queuemgr.core.ipc import update_job_state

class MyJob(QueueJobBase):
    def execute(self):
        # Your job logic here
        for i in range(100):
            update_job_state(
                self._shared_state,
                progress=i,
                description=f"Processing item {i}"
            )
    
    def on_start(self):
        update_job_state(self._shared_state, description="Job started")
    
    def on_stop(self):
        update_job_state(self._shared_state, description="Job stopped")
    
    def on_end(self):
        update_job_state(self._shared_state, description="Job completed")
    
    def on_error(self, exc):
        update_job_state(self._shared_state, description=f"Job failed: {exc}")

# Create queue and job
registry = JsonlRegistry("my_registry.jsonl")
queue = JobQueue(registry)
job = MyJob("my-job-1", {"param": "value"})

# Add and start job
queue.add_job(job)
queue.start_job("my-job-1")

# Monitor progress
status = queue.get_job_status("my-job-1")
print(f"Status: {status.status}, Progress: {status.progress}%")
```

## Examples

See the `examples/` directory for complete examples:

- `simple_job.py` - Basic job creation and execution
- `error_handling_job.py` - Error handling and recovery
- `progress_job.py` - Long-running job with progress updates
- `registry_example.py` - Registry functionality and job history

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=queuemgr

# Format code
black queuemgr tests

# Lint code
flake8 queuemgr tests

# Type check
mypy queuemgr
```

## License

MIT License - see LICENSE file for details.

## Author

Vasiliy Zdanovskiy - vasilyvz@gmail.com
# vvz-queuemgr
