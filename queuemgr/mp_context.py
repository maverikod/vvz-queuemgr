"""
Local multiprocessing context for queuemgr.

This module provides a package-local ``multiprocessing`` context so that all
process, queue, event, and manager objects created by queuemgr use a
consistent, explicitly chosen start method. queuemgr never calls
``multiprocessing.set_start_method``: doing so would mutate global process
state and could break host applications (or other libraries) that rely on a
different global start method (e.g. consumers that pin ``fork`` globally).

Using ``multiprocessing.get_context(method)`` instead gives queuemgr its own
isolated context object without touching the process-wide default. The
default method is "spawn" because it does not fork the caller's process
(and therefore does not duplicate live threads, open sockets, epoll file
descriptors, or asyncio event loops running in a host application such as an
asyncio/hypercorn server). The method can be overridden via the
``QUEUEMGR_MP_START_METHOD`` environment variable for environments where
"spawn" is not viable.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import multiprocessing
import os
from multiprocessing.context import BaseContext
from typing import Optional

ENV_VAR_NAME = "QUEUEMGR_MP_START_METHOD"
DEFAULT_START_METHOD = "spawn"
VALID_START_METHODS = ("spawn", "forkserver", "fork")

_cached_context: Optional[BaseContext] = None
_cached_method: Optional[str] = None


def get_mp_context() -> BaseContext:
    """
    Return the multiprocessing context used by queuemgr for all mp objects.

    The start method is resolved from the ``QUEUEMGR_MP_START_METHOD``
    environment variable (defaulting to "spawn") and is read once; the
    resulting context is cached for the lifetime of the process. This
    function never calls ``multiprocessing.set_start_method`` and therefore
    never mutates the global multiprocessing default used by the host
    application or other libraries.

    Returns:
        BaseContext: A multiprocessing context created via
        ``multiprocessing.get_context(method)``.

    Raises:
        ValueError: If ``QUEUEMGR_MP_START_METHOD`` is set to a value other
            than "spawn", "forkserver", or "fork".
    """
    global _cached_context, _cached_method

    method = os.environ.get(ENV_VAR_NAME, DEFAULT_START_METHOD)

    if _cached_context is not None and method == _cached_method:
        return _cached_context

    if method not in VALID_START_METHODS:
        raise ValueError(
            f"Invalid {ENV_VAR_NAME}={method!r}; must be one of "
            f"{VALID_START_METHODS}"
        )

    _cached_context = multiprocessing.get_context(method)
    _cached_method = method
    return _cached_context
