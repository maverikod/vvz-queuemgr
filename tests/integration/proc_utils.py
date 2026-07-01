"""
Process-tree inspection helpers for queuemgr integration tests.

All helpers here are Linux-only (/proc based), matching the queuemgr package
itself, which only targets Linux.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import os
import socket
import time
from typing import Callable, Iterable, List, Optional, Set, TypeVar

T = TypeVar("T")


def read_ppid(pid: int) -> Optional[int]:
    """
    Read the parent PID of a process from /proc/<pid>/stat.

    Args:
        pid: Process id to inspect.

    Returns:
        Parent pid, or None if the process is gone or unreadable.
    """
    try:
        with open(f"/proc/{pid}/stat", "r") as fh:
            content = fh.read()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None

    # Fields after the comm field (which is parenthesized and may contain
    # spaces) are space separated; ppid is the field right after the comm's
    # closing paren.
    try:
        after_comm = content.rsplit(")", 1)[1].strip()
        fields = after_comm.split()
        # fields[0] = state, fields[1] = ppid
        return int(fields[1])
    except (IndexError, ValueError):
        return None


def list_all_pids() -> List[int]:
    """
    List all numeric PIDs currently visible under /proc.

    Returns:
        List of process ids.
    """
    pids = []
    for name in os.listdir("/proc"):
        if name.isdigit():
            pids.append(int(name))
    return pids


def find_descendant_pids(root_pid: int) -> Set[int]:
    """
    Find all transitive descendants (children, grandchildren, ...) of a pid.

    Args:
        root_pid: PID whose descendants should be discovered.

    Returns:
        Set of descendant pids (root_pid itself excluded).
    """
    all_pids = list_all_pids()
    parent_of = {}
    for pid in all_pids:
        ppid = read_ppid(pid)
        if ppid is not None:
            parent_of[pid] = ppid

    descendants: Set[int] = set()
    changed = True
    while changed:
        changed = False
        for pid, ppid in parent_of.items():
            if pid in descendants:
                continue
            if ppid == root_pid or ppid in descendants:
                descendants.add(pid)
                changed = True
    return descendants


def pid_exists(pid: int) -> bool:
    """
    Check whether a pid is currently present under /proc.

    Args:
        pid: Process id to check.

    Returns:
        True if /proc/<pid> exists, False otherwise.
    """
    return os.path.exists(f"/proc/{pid}")


def fd_target_inodes(pid: int) -> Set[str]:
    """
    Collect the socket inode identifiers referenced by a process's open fds.

    Args:
        pid: Process id to inspect.

    Returns:
        Set of strings like "socket:[12345]" for every socket fd found. Empty
        set if the process is gone or the fd directory cannot be read (e.g.
        permission, or process exited mid-scan).
    """
    inodes: Set[str] = set()
    fd_dir = f"/proc/{pid}/fd"
    try:
        names = os.listdir(fd_dir)
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return inodes

    for name in names:
        try:
            target = os.readlink(os.path.join(fd_dir, name))
        except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
            continue
        if target.startswith("socket:["):
            inodes.add(target)
    return inodes


def socket_inode(sock: socket.socket) -> Optional[str]:
    """
    Determine the inode identifier ("socket:[N]") for an open socket, by
    cross referencing the current process's own /proc/self/fd entries.

    Args:
        sock: Open socket object owned by the current process.

    Returns:
        The "socket:[N]" string, or None if not found (should not normally
        happen for a valid open socket).
    """
    target_fd = sock.fileno()
    try:
        link = os.readlink(f"/proc/self/fd/{target_fd}")
    except OSError:
        return None
    if link.startswith("socket:["):
        return link
    return None


def wait_until(
    predicate: Callable[[], bool],
    timeout: float,
    interval: float = 0.05,
    description: str = "condition",
) -> None:
    """
    Poll a predicate until it returns True or the timeout elapses.

    Args:
        predicate: Zero-arg callable returning True when the wait is over.
        timeout: Maximum time to wait, in seconds.
        interval: Poll interval, in seconds.
        description: Human readable description used in the timeout error.

    Raises:
        AssertionError: If the predicate never became True within timeout.
    """
    deadline = time.time() + timeout
    last_exc: Optional[BaseException] = None
    while time.time() < deadline:
        try:
            if predicate():
                return
            last_exc = None
        except AssertionError as exc:
            last_exc = exc
        time.sleep(interval)
    if last_exc is not None:
        raise AssertionError(
            f"Timed out after {timeout}s waiting for {description}: {last_exc}"
        )
    raise AssertionError(f"Timed out after {timeout}s waiting for {description}")


def poll_for_value(
    getter: Callable[[], T],
    is_ready: Callable[[T], bool],
    timeout: float,
    interval: float = 0.05,
    description: str = "value",
) -> T:
    """
    Poll a getter until is_ready(value) is True, then return that value.

    Args:
        getter: Zero-arg callable producing the current value.
        is_ready: Predicate over the value indicating readiness.
        timeout: Maximum time to wait, in seconds.
        interval: Poll interval, in seconds.
        description: Human readable description used in the timeout error.

    Returns:
        The last fetched value once is_ready(value) is True.

    Raises:
        AssertionError: If is_ready never became True within timeout.
    """
    deadline = time.time() + timeout
    last_value: Optional[T] = None
    while time.time() < deadline:
        last_value = getter()
        if is_ready(last_value):
            return last_value
        time.sleep(interval)
    raise AssertionError(
        f"Timed out after {timeout}s waiting for {description}; "
        f"last value={last_value!r}"
    )
