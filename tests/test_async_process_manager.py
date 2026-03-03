"""
Tests for AsyncProcessManager command timeouts and control behaviour.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict

import pytest

from queuemgr.async_process_manager import AsyncProcessManager
from queuemgr.core.exceptions import ProcessControlError
from queuemgr.process_config import ProcessManagerConfig


class _DummyQueue:
    """Minimal queue stub exposing ``put`` used by AsyncProcessManager."""

    def __init__(self) -> None:
        self.last_value: Any | None = None

    def put(self, value: Any) -> None:
        """Store last value for inspection in tests."""
        self.last_value = value


class _FastResponseManager(AsyncProcessManager):
    """Manager subclass that immediately returns a successful result."""

    async def _get_response_async(self) -> Dict[str, Any]:  # type: ignore[override]
        return {"status": "success", "result": {"ok": True}}


class _SlowResponseManager(AsyncProcessManager):
    """Manager subclass that delays responses to exercise timeout handling."""

    def __init__(self, config: ProcessManagerConfig, delay: float) -> None:
        super().__init__(config)
        self._delay = delay

    async def _get_response_async(self) -> Dict[str, Any]:  # type: ignore[override]
        await asyncio.sleep(self._delay)
        return {"status": "success", "result": {"ok": True}}


def _prepare_manager(manager: AsyncProcessManager) -> None:
    """
    Prepare a manager instance for direct _send_command_async testing.

    The test helpers avoid spawning subprocesses by injecting dummy queues
    and marking the manager as running. Also sets the command lock so
    _send_command_async can run.
    """
    dummy_queue = _DummyQueue()
    manager._control_queue = dummy_queue  # type: ignore[attr-defined]
    manager._response_queue = dummy_queue  # type: ignore[attr-defined]
    manager._is_running = True  # type: ignore[attr-defined]
    manager._command_lock = asyncio.Lock()  # type: ignore[attr-defined]


def test_send_command_uses_config_command_timeout() -> None:
    """
    Ensure _send_command_async uses the configured command_timeout by default.
    """

    async def runner() -> None:
        config = ProcessManagerConfig(command_timeout=0.5)
        manager = _FastResponseManager(config)
        _prepare_manager(manager)

        result = await manager._send_command_async("test", {"value": 1})
        assert result == {"ok": True}

    asyncio.run(runner())


def test_send_command_uses_per_call_timeout_override() -> None:
    """
    Ensure per-call timeout parameter overrides the default command_timeout.
    """

    async def runner() -> None:
        # Large config timeout, but very small per-call timeout.
        config = ProcessManagerConfig(command_timeout=10.0)
        manager = _SlowResponseManager(config=config, delay=0.2)
        _prepare_manager(manager)

        with pytest.raises(ProcessControlError):
            await manager._send_command_async("test", {"value": 1}, timeout=0.05)

    start_time = time.perf_counter()
    asyncio.run(runner())
    elapsed = time.perf_counter() - start_time

    # We expect the override timeout to be honoured rather than the config value.
    assert elapsed < 2.0


async def _control_path_add_job_status_stop(
    registry_path: str,
    command_timeout: float = 10.0,
    num_status_polls: int = 5,
    job_duration_sec: int = 2,
) -> None:
    """
    Full control path: add_job -> start -> multiple get_job_status -> stop_job.
    Uses real AsyncProcessManager; no manager timeout errors; each op bounded.
    """
    from queuemgr.async_process_manager import async_queue_system
    from queuemgr.examples.simple_manager_example import SimpleJob

    async with async_queue_system(
        registry_path=registry_path,
        shutdown_timeout=15.0,
        command_timeout=command_timeout,
    ) as manager:
        await manager.add_job(
            SimpleJob, "control-path-job", {"duration": job_duration_sec}
        )
        await manager.start_job("control-path-job")

        for _ in range(num_status_polls):
            status = await manager.get_job_status("control-path-job")
            assert status["job_id"] == "control-path-job"

        await manager.stop_job("control-path-job")


def test_control_path_add_job_multiple_get_status_stop_job_no_manager_timeout(
    tmp_path,
) -> None:
    """
    Full control path: add_job -> multiple get_job_status -> stop_job.
    No manager timeout errors; each operation bounded (<10s) or deterministic error.
    """
    registry_path = str(tmp_path / "registry.jsonl")
    t0 = time.perf_counter()
    asyncio.run(
        _control_path_add_job_status_stop(
            registry_path=registry_path,
            command_timeout=10.0,
            num_status_polls=5,
            job_duration_sec=2,
        )
    )
    elapsed = time.perf_counter() - t0
    assert elapsed < 60.0


async def _control_path_stress_long_running(
    registry_path: str,
    command_timeout: float = 10.0,
    delay_before_polls_sec: float = 2.0,
    num_status_polls: int = 5,
    job_duration_sec: int = 25,
) -> None:
    """
    Stress variant: long-running job active; after delay, multiple status
    polls then stop_job. No manager timeout; each op bounded or deterministic.
    """
    from queuemgr.async_process_manager import async_queue_system
    from queuemgr.examples.simple_manager_example import SimpleJob

    async with async_queue_system(
        registry_path=registry_path,
        shutdown_timeout=30.0,
        command_timeout=command_timeout,
    ) as manager:
        await manager.add_job(SimpleJob, "stress-job", {"duration": job_duration_sec})
        await manager.start_job("stress-job")

        await asyncio.sleep(delay_before_polls_sec)

        results = await asyncio.gather(
            *[manager.get_job_status("stress-job") for _ in range(num_status_polls)]
        )
        for status in results:
            assert status["job_id"] == "stress-job"

        try:
            await manager.stop_job("stress-job")
        except ProcessControlError as e:
            # Bounded outcome: no manager timeout. Job may fail to stop within
            # stop_job_wait_timeout if it does not check STOP (e.g. long sleep).
            if "Command timed out waiting for response" in str(e):
                raise  # Manager timeout is the bug we are guarding against.
            # "Job failed to stop within timeout" is acceptable (deterministic).


def test_control_path_stress_long_running_job_status_polls_then_stop(
    tmp_path,
) -> None:
    """
    Stress: long-running job active; after delay, 3-5 status polls then stop_job.
    No manager timeout; same pass criteria (bounded or deterministic error).
    """
    registry_path = str(tmp_path / "registry_stress.jsonl")
    t0 = time.perf_counter()
    # stop_job in manager can wait up to stop_job_wait_timeout (10s);
    # use >10s so client does not time out.
    asyncio.run(
        _control_path_stress_long_running(
            registry_path=registry_path,
            command_timeout=15.0,
            delay_before_polls_sec=2.0,
            num_status_polls=5,
            job_duration_sec=25,
        )
    )
    elapsed = time.perf_counter() - t0
    assert elapsed < 90.0
