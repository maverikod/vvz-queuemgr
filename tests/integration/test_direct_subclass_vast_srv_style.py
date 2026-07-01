"""
vast_srv style consumer: a plain, stateless direct QueueJobBase subclass.

vast_srv's ai_admin/jobs define direct subclasses of QueueJobBase with no
additional instance state beyond job_id/params (no custom __init__, no extra
attributes). This test exercises the full lifecycle for such a class under
the (spawn-by-default) queuemgr multiprocessing context: add -> start ->
completed, verifying the result and that the job actually executed in a
separate process (distinct pid).

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import asyncio
import os
import time

from queuemgr import AsyncQueueSystem

from .jobs_common import DirectSubclassJob


async def _wait_for_terminal(queue: AsyncQueueSystem, job_id: str, timeout: float = 15.0):
    """Poll get_job_status until the job reaches a terminal state."""
    deadline = time.time() + timeout
    status = None
    while time.time() < deadline:
        status = await queue.get_job_status(job_id)
        if status["status"] in ("COMPLETED", "ERROR", "STOPPED", "DELETED"):
            return status
        await asyncio.sleep(0.05)
    raise AssertionError(f"job {job_id!r} never reached terminal state; last={status!r}")


def test_direct_subclass_full_lifecycle(tmp_path) -> None:
    """Full add_job -> start_job -> completed cycle for a bare subclass."""

    async def runner() -> None:
        registry_path = str(tmp_path / "registry.jsonl")
        queue = AsyncQueueSystem(registry_path=registry_path)
        await queue.start()
        try:
            await queue.add_job(
                DirectSubclassJob, "direct-1", {"numbers": [10, 20, 30]}
            )
            await queue.start_job("direct-1")

            status = await _wait_for_terminal(queue, "direct-1")
            assert status["status"] == "COMPLETED", status

            result = status["result"]
            assert result["sum"] == 60
            assert isinstance(result["pid"], int)
            assert result["pid"] != os.getpid(), (
                "Job executed in the same process as the test; expected a "
                "dedicated child process"
            )
        finally:
            await queue.stop()

    asyncio.run(runner())


def test_direct_subclass_multiple_instances_do_not_share_state(tmp_path) -> None:
    """
    Two concurrently running instances of the same stateless subclass must
    not leak state between each other (each gets its own params/result).
    """

    async def runner() -> None:
        registry_path = str(tmp_path / "registry.jsonl")
        queue = AsyncQueueSystem(registry_path=registry_path)
        await queue.start()
        try:
            await queue.add_job(DirectSubclassJob, "direct-a", {"numbers": [1, 1, 1]})
            await queue.add_job(DirectSubclassJob, "direct-b", {"numbers": [2, 2, 2]})
            await queue.start_job("direct-a")
            await queue.start_job("direct-b")

            status_a = await _wait_for_terminal(queue, "direct-a")
            status_b = await _wait_for_terminal(queue, "direct-b")

            assert status_a["status"] == "COMPLETED"
            assert status_b["status"] == "COMPLETED"
            assert status_a["result"]["sum"] == 3
            assert status_b["result"]["sum"] == 6
            assert status_a["result"]["pid"] != status_b["result"]["pid"]
        finally:
            await queue.stop()

    asyncio.run(runner())
