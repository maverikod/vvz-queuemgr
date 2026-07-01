"""
mcp_proxy_adapter-style job pattern: command/params/context/auto_import.

mcp_proxy_adapter calls ``add_job(JobClass, job_id, params_dict)`` where
params is a JSON-like dict:
    {"command": "...", "params": {...}, "context": {...},
     "auto_import_modules": ["json", "os"]}

This test exercises the full add_job -> start_job -> poll-to-completion ->
result cycle with a job that performs auto-imports and dispatches a "command"
by name (see command_execution_job.py), and separately pins the current
(documented) behaviour when params contain a non-picklable object.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import asyncio
import time

import pytest

from queuemgr import AsyncQueueSystem
from queuemgr.exceptions import ProcessControlError, QueueManagerError

from .command_execution_job import CommandExecutionJob, UnpicklableParamsJob


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


def test_command_execution_job_full_cycle(tmp_path) -> None:
    """
    add_job -> start_job -> completed, with auto_import_modules honoured and
    the dispatched "sum" command producing the expected result.
    """

    async def runner() -> None:
        registry_path = str(tmp_path / "registry.jsonl")
        queue = AsyncQueueSystem(registry_path=registry_path)
        await queue.start()
        try:
            params = {
                "command": "sum",
                "params": {"numbers": [1, 2, 3, 4, 5]},
                "context": {"request_id": "abc-123"},
                "auto_import_modules": ["json", "os"],
            }
            await queue.add_job(CommandExecutionJob, "cmd-sum-1", params)
            await queue.start_job("cmd-sum-1")

            status = await _wait_for_terminal(queue, "cmd-sum-1")
            assert status["status"] == "COMPLETED", status

            result = status["result"]
            assert result["success"] is True
            assert result["value"] == 15
            assert set(result["imported_modules"]) == {"json", "os"}
        finally:
            await queue.stop()

    asyncio.run(runner())


def test_command_execution_job_unknown_command_reports_failure(tmp_path) -> None:
    """
    An unknown command name results in a native-error result payload
    (success=False), which queuemgr's own native-error detection
    (QueueJobBase._extract_native_error_message) promotes to a job-level
    ERROR status automatically -- even though execute() itself returned
    normally without raising. This is documented, intentional behavior in
    jobs/base_core.py (_handle_completion / _extract_native_error_message),
    not a bug: any job result dict shaped like {"success": False, ...} is
    treated as a command-level failure.
    """

    async def runner() -> None:
        registry_path = str(tmp_path / "registry.jsonl")
        queue = AsyncQueueSystem(registry_path=registry_path)
        await queue.start()
        try:
            params = {
                "command": "does-not-exist",
                "params": {},
                "auto_import_modules": [],
            }
            await queue.add_job(CommandExecutionJob, "cmd-unknown-1", params)
            await queue.start_job("cmd-unknown-1")

            status = await _wait_for_terminal(queue, "cmd-unknown-1")
            assert status["status"] == "ERROR", status
            assert "Unknown command" in status["description"]
        finally:
            await queue.stop()

    asyncio.run(runner())


def test_unpicklable_params_contract(tmp_path) -> None:
    """
    Pin current (documented) behaviour for non-picklable params (e.g. a
    lambda passed by a careless caller).

    Contract as currently implemented: AsyncQueueSystem.add_job sends the
    job spec (job_class, job_id, params) over a multiprocessing.Queue to the
    manager subprocess. multiprocessing.Queue.put() pickles the payload on a
    background feeder thread; when that pickling fails, the exception is
    swallowed inside multiprocessing internals (logged to stderr only) and
    the item is silently dropped -- put() itself does not raise. As a
    result, add_job() does NOT raise for unpicklable params; instead the
    manager never receives the add_job command, the response never arrives,
    and add_job() eventually raises ProcessControlError due to a
    control-plane timeout.

    This test intentionally uses a short timeout to keep runtime bounded and
    asserts on that timeout-shaped failure. If a future fix makes add_job
    validate picklability eagerly and raise synchronously, this test's
    expectations should be revisited (that would be a behavior
    improvement, not a regression).

    API note (discovered while writing this test): queuemgr currently
    defines TWO distinct, unrelated ``ProcessControlError`` classes that
    both subclass ``QueueManagerError`` but not each other:
    ``queuemgr.exceptions.ProcessControlError`` (what ``queuemgr.
    ProcessControlError`` re-exports, and what simple_api/async_simple_api/
    jobs/base_core.py raise) and ``queuemgr.core.exceptions.
    ProcessControlError`` (what async_process_manager*/process_core raise
    for control-plane failures, including this timeout). A caller who does
    ``except queuemgr.ProcessControlError`` will NOT catch the exception
    actually raised by ``AsyncQueueSystem.add_job``/``get_job_status`` on a
    control-plane timeout, because that path raises the ``core.exceptions``
    variant. Only the shared ``QueueManagerError`` base reliably catches
    both. This test therefore asserts against ``QueueManagerError`` (and
    separately confirms which concrete class is actually raised).
    """

    async def runner() -> None:
        registry_path = str(tmp_path / "registry.jsonl")
        queue = AsyncQueueSystem(registry_path=registry_path)
        await queue.start()
        try:
            unpicklable_params = {
                "command": "sum",
                "params": {"numbers": [1, 2, 3]},
                "auto_import_modules": [],
                "callback": lambda x: x,  # not picklable
            }

            with pytest.raises(QueueManagerError) as excinfo:
                await queue.add_job(
                    UnpicklableParamsJob,
                    "cmd-unpicklable-1",
                    unpicklable_params,
                    timeout=3.0,
                )

            message = str(excinfo.value)
            assert "timed out" in message.lower() or "timeout" in message.lower(), (
                "Expected a control-plane timeout error for unpicklable "
                f"params, got: {message}"
            )
            # Document exactly which concrete class is raised in practice:
            # it is NOT the publicly re-exported queuemgr.ProcessControlError.
            assert not isinstance(excinfo.value, ProcessControlError), (
                "If this now fails, the two ProcessControlError classes may "
                "have been unified -- update this test's docstring/comment, "
                "it would be a welcome API fix."
            )

            # The job was never actually registered in the manager (the
            # add_job command never arrived), so querying its status must
            # raise rather than report a real status.
            with pytest.raises(QueueManagerError):
                await queue.get_job_status("cmd-unpicklable-1")
        finally:
            await queue.stop()

    asyncio.run(runner())
