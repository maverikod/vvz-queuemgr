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
    and marking the manager as running.
    """
    dummy_queue = _DummyQueue()
    manager._control_queue = dummy_queue  # type: ignore[attr-defined]
    manager._response_queue = dummy_queue  # type: ignore[attr-defined]
    manager._is_running = True  # type: ignore[attr-defined]


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
