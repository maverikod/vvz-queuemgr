Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com

## Queue Stop Regression Observation

Current live verification:
qa_sleep use_queue=true with seconds=30 and tick_seconds=0.5 was started, then queue_stop_job was called. queue_stop_job returned success=true, status=stopped, actual_status=stopped, stopped=true. queue_get_job_status returned outer status=stopped and job_success=false, and queue_list_jobs(status_filter=stopped) contained the job. queue_get_job_logs worked. However, the retained result still contained result.status=completed, result.result.success=true, and result.result.data.slept_seconds=30.0. Logs contained heartbeats from elapsed=0.0s through elapsed=29.5s. This means the job appears to have completed the full requested sleep successfully while being reported as stopped.
