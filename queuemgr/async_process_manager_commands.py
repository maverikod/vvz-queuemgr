"""
Command send/response helpers for AsyncProcessManager (IPC).

Serialized send+wait logic and context manager live here to keep
async_process_manager.py under the line limit.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import asyncio
import logging
from queue import Empty
from multiprocessing import Queue
from typing import Any, Dict, Optional

from queuemgr.core.exceptions import ProcessControlError

from .process_config import ProcessManagerConfig

logger = logging.getLogger("queuemgr.async_process_manager")


async def get_response_async(
    response_queue: Queue, timeout_seconds: float
) -> Dict[str, Any]:
    """
    Read one message from the response queue asynchronously.

    Waits up to timeout_seconds (respects configured command_timeout).
    Raises asyncio.TimeoutError if no message within timeout_seconds.
    """
    loop = asyncio.get_event_loop()

    def get_response() -> Optional[Dict[str, Any]]:
        try:
            return response_queue.get(timeout=timeout_seconds)
        except Empty:
            return None

    result = await loop.run_in_executor(None, get_response)
    if result is None:
        raise asyncio.TimeoutError("No response received")
    return result


async def send_command_async(
    control_queue: Queue,
    response_queue: Queue,
    lock: asyncio.Lock,
    config: ProcessManagerConfig,
    command: str,
    params: Dict[str, Any],
    timeout: Optional[float],
) -> Any:
    """
    Send a command and wait for response under the given lock.

    Serializes with the lock so concurrent callers do not mix responses.
    The timeout is control-plane only (IPC round trip); wait uses
    effective_timeout fully (no internal cap).
    """
    effective_timeout = timeout if timeout is not None else config.command_timeout

    try:
        async with lock:
            if not control_queue or not response_queue:
                raise ProcessControlError("manager", command, "Queues not initialized")

            loop = asyncio.get_event_loop()

            def put_command() -> None:
                control_queue.put({"command": command, "params": params})

            await loop.run_in_executor(None, put_command)

            try:
                response = await get_response_async(response_queue, effective_timeout)
            except asyncio.TimeoutError as exc:
                logger.warning(
                    "Async manager command '%s' timed out after %.1fs",
                    command,
                    effective_timeout,
                )
                raise ProcessControlError(
                    "manager",
                    command,
                    "Command timed out waiting for response",
                ) from exc

        if response.get("status") == "error":
            error_message = response.get("error", "Unknown error")
            logger.error(
                "Async manager command '%s' failed inside manager: %s",
                command,
                error_message,
            )
            raise ProcessControlError("manager", command, error_message)

        return response.get("result")

    except asyncio.TimeoutError as exc:
        logger.warning("Async manager command '%s' exceeded response timeout", command)
        raise ProcessControlError(
            "manager", command, "Command timed out waiting for response"
        ) from exc
    except ProcessControlError:
        raise
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Async manager command '%s' failed unexpectedly: %s", command, exc)
        raise ProcessControlError("manager", command, f"Command failed: {exc}") from exc
