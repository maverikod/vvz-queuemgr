# Release Notes: queuemgr 1.0.11

**Date**: 2025-01-27  
**Author**: Vasiliy Zdanovskiy  
**Email**: vasilyvz@gmail.com

## Summary

This release adds job log capture functionality, allowing clients to retrieve stdout/stderr output from running and completed jobs. The release also includes code quality improvements with file size optimizations.

## New Features

### Job Log Capture

- **Added `get_job_logs()` method** to `JobQueue`, `AsyncQueueSystem`, `ProcQueueSystem`, and related APIs
  - Retrieves stdout and stderr logs for any job
  - Returns logs as `{"stdout": List[str], "stderr": List[str]}`
  - Works for both running and completed jobs

- **Automatic log capture** in job execution
  - `QueueJobBase` now automatically captures stdout/stderr during job execution
  - Logs are stored in shared state for IPC access
  - Logs persist after job completion

- **New `LogCapture` class** in `queuemgr.jobs.log_capture`
  - File-like object for capturing output to shared lists
  - Handles line buffering and newline splitting
  - Thread-safe for multiprocessing environments

## Code Quality Improvements

- **File size optimizations**
  - Extracted `LogCapture` class to separate module (`queuemgr/jobs/log_capture.py`)
  - Moved `get_job_logs()` to `JobQueueMetricsMixin` for better organization
  - Reduced `base_core.py` from 413 to 364 lines
  - Reduced `job_queue.py` from 439 to 400 lines (within project limits)

- **Fixed code_mapper line counting**
  - Updated `code_analysis/analyzer.py` to correctly handle trailing newlines
  - Ensures accurate file size reporting

## API Changes

### New Methods

```python
# JobQueue
def get_job_logs(self, job_id: JobId) -> Dict[str, List[str]]

# AsyncQueueSystem
async def get_job_logs(self, job_id: str) -> Dict[str, List[str]]

# ProcQueueSystem
def get_job_logs(self, job_id: str) -> Dict[str, List[str]]
```

### New Module

- `queuemgr.jobs.log_capture` - `LogCapture` class for output capture

## Internal Changes

- Updated `create_job_shared_state()` in `ipc_manager.py` to use `"stdout"` and `"stderr"` keys (instead of `"stdout_logs"`/`"stderr_logs"`)
- Updated `_job_loop()` in `base_core.py` to capture stdout/stderr using `LogCapture`
- Added log retrieval to `process_commands.py` and `proc_manager_bootstrap.py`

## Testing

- Added comprehensive tests in `tests/queue/test_job_queue_logs.py`
  - Test empty logs for new jobs
  - Test log capture during execution
  - Test log retrieval after completion
  - Test error handling for non-existent jobs

## Migration Guide

No breaking changes. Existing code continues to work without modifications.

To use the new log capture feature:

```python
from queuemgr import AsyncQueueSystem

queue = AsyncQueueSystem()
await queue.start()

# Add and start a job
job_id = await queue.add_job(MyJob, "job-1", {})
await queue.start_job(job_id)

# Wait for execution
await asyncio.sleep(1)

# Get logs
logs = await queue.get_job_logs(job_id)
print("Stdout:", logs["stdout"])
print("Stderr:", logs["stderr"])
```

## Bug Fixes

- Fixed shared state key naming consistency (`stdout`/`stderr` vs `stdout_logs`/`stderr_logs`)
- Fixed code_mapper line counting to correctly handle files ending with newline

## Dependencies

No new dependencies. All functionality uses standard library only.

## Files Changed

### New Files
- `queuemgr/jobs/log_capture.py` - Log capture utility class
- `tests/queue/test_job_queue_logs.py` - Tests for log functionality
- `docs/release-1-0-11.md` - This file

### Modified Files
- `queuemgr/jobs/base_core.py` - Added log capture in `_job_loop()`
- `queuemgr/core/ipc_manager.py` - Updated shared state keys
- `queuemgr/queue/job_queue.py` - Removed `get_job_logs()` (moved to mixin)
- `queuemgr/queue/job_queue_metrics.py` - Added `get_job_logs()` method
- `queuemgr/process_commands.py` - Added `get_job_logs` command handler
- `queuemgr/async_process_manager.py` - Added `get_job_logs()` method
- `queuemgr/async_simple_api.py` - Added `get_job_logs()` method
- `queuemgr/proc_manager_core.py` - Added `get_job_logs()` method
- `queuemgr/proc_api.py` - Added `get_job_logs()` method
- `queuemgr/proc_manager_bootstrap.py` - Added `get_job_logs` command handler
- `code_analysis/analyzer.py` - Fixed line counting logic

## Contributors

- Vasiliy Zdanovskiy

## Related Issues

- Feature request: Job log retrieval for monitoring long-running tasks
- Code quality: File size optimization to meet project standards

