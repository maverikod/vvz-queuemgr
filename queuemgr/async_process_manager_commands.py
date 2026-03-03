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
from multiprocessing import Queue
from typing import Any, Awaitable, Callable, Dict, Optional

from queuemgr.core.exceptions import ProcessControlError

from .process_config import ProcessManagerConfig

logger = logging.getLogger("queuemgr.async_process_manager")


async def get_response_async(response_queue: Queue) -> Dict[str, Any]:
    """
    Read one message from the response queue asynchronously.

    Polls with short timeouts to avoid blocking the event loop.
    Raises asyncio.TimeoutError if no message within ~10s.
    """
    loop = asyncio.get_event_loop()

    def get_response() -> Optional[Dict[str, Any]]:
        try:
            return response_queue.get(timeout=0.1)
        except Exception:
            return None

    for _ in range(100):
        result = await loop.run_in_executor(None, get_response)
        if result is not None:
            return result
        await asyncio.sleep(0.1)

    raise asyncio.TimeoutError("No response received")


async def send_command_async(
    control_queue: Queue,
    response_queue: Queue,
    lock: asyncio.Lock,
    config: ProcessManagerConfig,
    command: str,
    params: Dict[str, Any],
    timeout: Optional[float],
    get_response_coro: Callable[[], Awaitable[Dict[str, Any]]],
) -> Any:
    """
    Send a command and wait for response under the given lock.

    Serializes with the lock so concurrent callers do not mix responses.
    The timeout is control-plane only (IPC round trip).
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
                response = await asyncio.wait_for(
                    get_response_coro(), timeout=effective_timeout
                )
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
