# Release Notes for queuemgr 1.0.10

## Overview
This release adds job timing tracking (start and completion timestamps) and configurable retention for completed jobs, addressing the critical bug where completed jobs were disappearing immediately after completion.

## Key Changes

### 1. Job Timing Tracking
- **Added `started_at` and `completed_at` fields to `JobRecord`**: Each job now tracks when it started execution and when it completed (or errored).
- **Automatic timestamp tracking**: 
  - `started_at` is set when a job transitions to `RUNNING` status
  - `completed_at` is set when a job transitions to `COMPLETED` or `ERROR` status
- **Registry persistence**: Timestamps are serialized/deserialized in the registry file, ensuring persistence across restarts.

### 2. Configurable Completed Job Retention
- **New parameter `completed_job_retention_seconds`**: Added to `ProcessManagerConfig`, `ProcManagerConfig`, `JobQueue`, and `AsyncQueueSystem`.
- **Smart cleanup logic**: 
  - If `completed_job_retention_seconds` is `None`, completed jobs are preserved indefinitely (allows clients to retrieve results).
  - If set to a value (e.g., 3600 for 1 hour), completed/error jobs are automatically removed after the specified time.
  - Jobs are only removed if they have been completed for longer than the retention period.
- **Backward compatible**: Default behavior preserves completed jobs (retention is `None` by default).

### 3. Enhanced Cleanup Logic
- **Time-based cleanup**: `cleanup_completed_jobs()` now checks `completed_at` timestamp before removing jobs.
- **Prevents premature deletion**: Jobs that just completed (no `completed_at` set yet) are never removed.
- **Respects retention settings**: Only removes jobs that have exceeded the retention period.

### 4. Bug Fixes
- **Fixed immediate deletion of completed jobs**: Previously, completed jobs were removed immediately after completion, even when `max_queue_size=None` and `per_job_type_limits=None`. Now they are preserved unless retention time expires.
- **Improved job result retrieval**: Clients can now reliably retrieve results from completed jobs after they finish.

## API Changes

### New Parameters
- `AsyncQueueSystem.__init__()`: Added `completed_job_retention_seconds: Optional[float] = None`
- `JobQueue.__init__()`: Added `completed_job_retention_seconds: Optional[float] = None`
- `ProcessManagerConfig`: Added `completed_job_retention_seconds: Optional[float] = None`
- `ProcManagerConfig`: Added `completed_job_retention_seconds: Optional[float] = None`

### Updated Types
- `JobRecord`: Added optional fields `started_at: Optional[datetime]` and `completed_at: Optional[datetime]`

## Usage Example

```python
from queuemgr import AsyncQueueSystem

# Preserve completed jobs indefinitely (default)
queue_system = AsyncQueueSystem(
    registry_path="queue.jsonl",
    completed_job_retention_seconds=None  # Keep forever
)

# Auto-remove completed jobs after 1 hour
queue_system = AsyncQueueSystem(
    registry_path="queue.jsonl",
    completed_job_retention_seconds=3600.0  # 1 hour
)

await queue_system.start()

# Add and start a job
job_id = await queue_system.add_job(MyJob, "job-1", {})
await queue_system.start_job(job_id)

# Wait for completion
await asyncio.sleep(5)

# Get job status with timing information
status = await queue_system.get_job_status(job_id)
print(f"Started at: {status.started_at}")
print(f"Completed at: {status.completed_at}")
print(f"Duration: {status.completed_at - status.started_at}")
```

## Upgrade Notes
- No breaking changes: All new parameters are optional with sensible defaults.
- Existing code continues to work without modifications.
- If you want to preserve completed jobs, no changes needed (default behavior).
- If you want auto-cleanup, set `completed_job_retention_seconds` to your desired retention time.

## Contributors
- Vasiliy Zdanovskiy

## Full Changelog
Refer to the commit history for a detailed list of changes.

