# Release 1.0.7

**Author**: Vasiliy Zdanovskiy  
**Email**: vasilyvz@gmail.com  
**Date**: 2025-01-27

## Summary

Release 1.0.7 adds per-job-type memory limits functionality, allowing fine-grained control over queue memory usage by job type.

## New Features

### Per-Job-Type Memory Limits

Added support for per-job-type memory limits in queue configuration. When a limit is reached for a specific job type, the oldest job of that type is automatically removed (FIFO eviction) before adding a new job.

**Configuration Example:**

```python
from queuemgr import AsyncQueueSystem

queue = AsyncQueueSystem(
    registry_path="queue.jsonl",
    per_job_type_limits={
        "CommandExecutionJob": 100,
        "DataProcessingJob": 50,
        "FileOperationJob": 30,
        "ApiCallJob": 200,
    },
    max_queue_size=1000,  # Global fallback limit
)
```

**Features:**

- Per-job-type limits with automatic FIFO eviction
- Global queue size limit as fallback
- Automatic eviction of oldest jobs when limits are reached
- Thread-safe implementation
- Backward compatible (optional feature)

## API Changes

### `ProcessManagerConfig`

Added optional parameters:
- `max_queue_size: Optional[int]` - Global maximum number of jobs
- `per_job_type_limits: Optional[Dict[str, int]]` - Dict mapping job_type to max count

### `JobQueue.__init__()`

Added optional parameters:
- `max_queue_size: Optional[int]` - Global maximum number of jobs
- `per_job_type_limits: Optional[Dict[str, int]]` - Dict mapping job_type to max count

### `AsyncQueueSystem.__init__()`

Added optional parameters:
- `max_queue_size: Optional[int]` - Global maximum number of jobs
- `per_job_type_limits: Optional[Dict[str, int]]` - Dict mapping job_type to max count

## Implementation Details

- Job type is determined from class name (`job.__class__.__name__`)
- Eviction uses FIFO (First In, First Out) strategy
- Per-job-type limits take precedence over global limit
- Evicted jobs are properly removed from registry
- Logging added for eviction events

## Testing

Added comprehensive test suite in `tests/queue/test_job_queue_per_type_limits.py`:
- 11 test cases covering all scenarios
- Tests for per-type limits, global limits, eviction order, edge cases

## Backward Compatibility

Fully backward compatible. Existing code continues to work without changes. New parameters are optional.

## Files Changed

- `queuemgr/process_config.py` - Added configuration parameters
- `queuemgr/queue/job_queue.py` - Added eviction logic and helper methods
- `queuemgr/async_process_manager.py` - Updated to pass configuration
- `queuemgr/process_core.py` - Updated to pass configuration
- `queuemgr/proc_manager_core.py` - Updated to pass configuration
- `queuemgr/async_simple_api.py` - Added API parameters
- `tests/queue/test_job_queue_per_type_limits.py` - New test suite

