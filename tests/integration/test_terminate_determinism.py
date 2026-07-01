"""
Regression test for SIGTERM-handler re-installation breaking terminate/stop.

Production risk: queuemgr's async_simple_api module used to install SIGTERM
and SIGINT handlers at *import time* (module-level ``_setup_async_cleanup()``
call). If a spawned job child process re-imports queuemgr (which it must, to
locate the job class by reference), that import-time side effect would
re-run in the child, installing a signal handler that intercepts SIGTERM
inside the child and prevents/alters its normal termination -- breaking
deterministic stop/terminate semantics for long-running jobs.

This test starts a long-running, cooperative-but-terminable job through
AsyncQueueSystem, requests a stop, and asserts:
- the job process actually exits within a bounded timeout,
- the queue reports an honest terminal status (STOPPED),
- the pid is no longer present under /proc.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import asyncio
import time

from queuemgr import AsyncQueueSystem

from .jobs_common import SleepLoopJob
from .proc_utils import pid_exists


async def _wait_for_pid(queue: AsyncQueueSystem, job_id: str, timeout: float = 10.0) -> int:
    """Poll job status until a pid appears in the result payload."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = await queue.get_job_status(job_id)
        result = status.get("result")
        if isinstance(result, dict) and "pid" in result:
            return int(result["pid"])
        await asyncio.sleep(0.05)
    raise AssertionError(f"job {job_id!r} never reported a pid in its result")


def test_stop_job_kills_process_within_timeout(tmp_path) -> None:
    """
    stop_job on a long-running job must terminate the child process quickly
    (<10s), report STOPPED, and leave no trace of the pid under /proc.
    """

    async def runner() -> None:
        registry_path = str(tmp_path / "registry.jsonl")
        queue = AsyncQueueSystem(registry_path=registry_path)
        await queue.start()
        try:
            await queue.add_job(SleepLoopJob, "term-1", {})
            await queue.start_job("term-1")

            job_pid = await _wait_for_pid(queue, "term-1", timeout=10.0)
            assert pid_exists(job_pid), (
                f"job pid {job_pid} should exist right after it reported itself"
            )

            stop_start = time.perf_counter()
            await queue.stop_job("term-1", timeout=8.0)
            stop_elapsed = time.perf_counter() - stop_start

            assert stop_elapsed < 10.0, (
                f"stop_job took {stop_elapsed:.2f}s, expected < 10s "
                "(possible SIGTERM handler interference in the job child)"
            )

            status = await queue.get_job_status("term-1")
            assert status["status"] == "STOPPED", (
                f"Expected honest STOPPED status after stop_job, got: {status}"
            )

            # Give the OS a brief moment to reap/finish removing /proc entry
            # after the parent observed process exit; poll rather than sleep
            # blindly.
            deadline = time.time() + 5.0
            while time.time() < deadline and pid_exists(job_pid):
                await asyncio.sleep(0.05)

            assert not pid_exists(job_pid), (
                f"job process pid {job_pid} is still present under /proc "
                "after stop_job reported success and returned"
            )
        finally:
            await queue.stop()

    asyncio.run(runner())


def test_stop_job_is_deterministic_across_repeated_runs(tmp_path) -> None:
    """
    Run the stop/terminate cycle a few times back to back to catch
    non-deterministic hangs (e.g. an occasionally-reinstalled signal
    handler swallowing SIGTERM only sometimes).
    """

    async def runner() -> None:
        registry_path = str(tmp_path / "registry.jsonl")
        queue = AsyncQueueSystem(registry_path=registry_path)
        await queue.start()
        try:
            for i in range(3):
                job_id = f"term-repeat-{i}"
                await queue.add_job(SleepLoopJob, job_id, {})
                await queue.start_job(job_id)

                job_pid = await _wait_for_pid(queue, job_id, timeout=10.0)

                stop_start = time.perf_counter()
                await queue.stop_job(job_id, timeout=8.0)
                stop_elapsed = time.perf_counter() - stop_start

                assert stop_elapsed < 10.0, (
                    f"iteration {i}: stop_job took {stop_elapsed:.2f}s (>=10s)"
                )

                status = await queue.get_job_status(job_id)
                assert status["status"] == "STOPPED", (
                    f"iteration {i}: expected STOPPED, got {status}"
                )

                deadline = time.time() + 5.0
                while time.time() < deadline and pid_exists(job_pid):
                    await asyncio.sleep(0.05)
                assert not pid_exists(job_pid), (
                    f"iteration {i}: job pid {job_pid} still present after stop"
                )
        finally:
            await queue.stop()

    asyncio.run(runner())
