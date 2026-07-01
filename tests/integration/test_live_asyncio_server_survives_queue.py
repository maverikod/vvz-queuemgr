"""
Main casmgr repro: a live asyncio HTTP server must survive queuemgr activity.

Production incident: casmgr runs a FastAPI/hypercorn asyncio server. When
queuemgr forked that live process to execute queue jobs, the forked child
inherited the parent's listening socket fd / epoll / already-accepted
connection fds. After the queue's fork+fallback dance ran, the *parent's*
HTTP command API degraded permanently: any POST body was parsed as an empty
dict from then on.

This test reproduces the necessary conditions without hypercorn: it starts a
minimal asyncio TCP/HTTP echo server in the test process, opens a keep-alive
connection, and interleaves multiple queuemgr jobs (including a slow one)
between HTTP round trips. Every echo must match its request body exactly,
before, during, and after job execution -- both on the pre-existing
keep-alive connection and on brand-new connections. Additionally, while the
slow job is running, no descendant process may hold an fd on the server's
listening socket (the inode signature of "we forked a live listener").

Under the "fork" start method this test is expected to fail (or to expose a
corrupted parent) prior to the queuemgr mp_context fix landing everywhere;
under "spawn" (the queuemgr default) it must pass.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import asyncio
import os
import socket
import time

import pytest

from queuemgr import AsyncQueueSystem

from .http_echo_server import start_echo_server, send_http_post
from .jobs_common import QuickJob, SlowJob
from .proc_utils import (
    fd_target_inodes,
    find_descendant_pids,
    poll_for_value,
    socket_inode,
)


def _connect(host: str, port: int, timeout: float = 5.0) -> socket.socket:
    """Open a blocking TCP connection to the echo server."""
    sock = socket.create_connection((host, port), timeout=timeout)
    sock.settimeout(timeout)
    return sock


def test_http_echo_survives_interleaved_queue_jobs(tmp_path) -> None:
    """
    Keep-alive HTTP echo must stay byte-exact across queuemgr job execution.
    """

    async def runner() -> None:
        server_handle = await start_echo_server()
        try:
            registry_path = str(tmp_path / "registry.jsonl")
            queue = AsyncQueueSystem(registry_path=registry_path)
            await queue.start()
            try:
                loop = asyncio.get_running_loop()

                # 1. Baseline: open a keep-alive connection and exchange a
                #    few requests before touching the queue at all.
                keep_alive_sock = await loop.run_in_executor(
                    None, _connect, server_handle.host, server_handle.port
                )
                for i in range(3):
                    body = f"pre-queue-{i}".encode("ascii")
                    echoed = await loop.run_in_executor(
                        None,
                        send_http_post,
                        keep_alive_sock,
                        "/echo",
                        body,
                    )
                    assert echoed == body

                # 2. Kick off a slow job asynchronously and a couple of
                #    quick jobs, interleaving HTTP round trips on both the
                #    existing keep-alive connection and fresh connections.
                await queue.add_job(SlowJob, "slow-1", {"duration": 2.0})
                await queue.start_job("slow-1")

                for i in range(3):
                    await queue.add_job(QuickJob, f"quick-{i}", {})
                    await queue.start_job(f"quick-{i}")

                # While the slow job is (likely) still running, hammer the
                # HTTP server both on the pre-existing connection and on new
                # connections.
                for i in range(6):
                    body = f"during-queue-keepalive-{i}".encode("ascii")
                    echoed = await loop.run_in_executor(
                        None,
                        send_http_post,
                        keep_alive_sock,
                        "/echo",
                        body,
                    )
                    assert echoed == body, (
                        "Keep-alive echo mismatch during queue activity: "
                        f"sent {body!r} got {echoed!r}"
                    )

                    fresh_sock = await loop.run_in_executor(
                        None, _connect, server_handle.host, server_handle.port
                    )
                    try:
                        body2 = f"during-queue-fresh-{i}".encode("ascii")
                        echoed2 = await loop.run_in_executor(
                            None, send_http_post, fresh_sock, "/echo", body2
                        )
                        assert echoed2 == body2, (
                            "Fresh-connection echo mismatch during queue "
                            f"activity: sent {body2!r} got {echoed2!r}"
                        )
                    finally:
                        fresh_sock.close()

                    await asyncio.sleep(0.2)

                # 3. Wait for the slow job to actually finish, then run one
                #    more full request cycle to prove the parent's HTTP
                #    parsing is still healthy post-completion.
                async def _slow_done() -> bool:
                    status = await queue.get_job_status("slow-1")
                    return status["status"] in ("COMPLETED", "ERROR", "STOPPED")

                deadline = time.time() + 15.0
                while time.time() < deadline:
                    if await _slow_done():
                        break
                    await asyncio.sleep(0.1)
                else:
                    pytest.fail("slow-1 job never reached a terminal state")

                final_status = await queue.get_job_status("slow-1")
                assert final_status["status"] == "COMPLETED", final_status

                for i in range(3):
                    body = f"post-queue-keepalive-{i}".encode("ascii")
                    echoed = await loop.run_in_executor(
                        None,
                        send_http_post,
                        keep_alive_sock,
                        "/echo",
                        body,
                    )
                    assert echoed == body, (
                        "Keep-alive echo mismatch AFTER queue job completed "
                        f"(parent HTTP parser corrupted): sent {body!r} got "
                        f"{echoed!r}"
                    )

                for i in range(3):
                    fresh_sock = await loop.run_in_executor(
                        None, _connect, server_handle.host, server_handle.port
                    )
                    try:
                        body = f"post-queue-fresh-{i}".encode("ascii")
                        echoed = await loop.run_in_executor(
                            None, send_http_post, fresh_sock, "/echo", body
                        )
                        assert echoed == body, (
                            "Fresh-connection echo mismatch AFTER queue job "
                            f"completed: sent {body!r} got {echoed!r}"
                        )
                    finally:
                        fresh_sock.close()

                keep_alive_sock.close()
            finally:
                await queue.stop()
        finally:
            server_handle.server.close()
            await server_handle.server.wait_closed()

    asyncio.run(runner())


def test_no_descendant_holds_listener_fd_while_job_runs(tmp_path) -> None:
    """
    While a slow job runs, no descendant process may hold an fd referring to
    the server's listening socket inode.

    Under "fork", a naively forked manager/job process would inherit the
    listener fd (same socket inode as the parent's). Under "spawn" (the
    queuemgr default / the fix), child processes never share that fd table
    at all, so no descendant can reference the listener's inode.
    """

    async def runner() -> None:
        server_handle = await start_echo_server()
        try:
            listener_inode = socket_inode(server_handle.listen_socket)
            assert listener_inode is not None, (
                "Could not resolve listener socket inode via /proc/self/fd"
            )

            registry_path = str(tmp_path / "registry.jsonl")
            queue = AsyncQueueSystem(registry_path=registry_path)
            await queue.start()
            try:
                main_pid = os.getpid()

                await queue.add_job(SlowJob, "slow-fd-1", {"duration": 3.0})
                await queue.start_job("slow-fd-1")

                # Poll: while the job is running, inspect every descendant
                # process's fd table for the listener inode. We sample
                # repeatedly across the job's lifetime (not just once) to
                # maximize the chance of observing a leaked fd if one exists.
                offending = []
                samples = 0
                deadline = time.time() + 3.5
                while time.time() < deadline:
                    descendants = find_descendant_pids(main_pid)
                    for pid in descendants:
                        inodes = fd_target_inodes(pid)
                        if listener_inode in inodes:
                            offending.append((pid, listener_inode))
                    samples += 1
                    status = await queue.get_job_status("slow-fd-1")
                    if status["status"] in ("COMPLETED", "ERROR", "STOPPED"):
                        break
                    await asyncio.sleep(0.1)

                assert samples > 0, "No samples were taken; test setup issue"
                assert not offending, (
                    "Descendant process(es) hold an fd on the server's "
                    f"listening socket (inode {listener_inode}): {offending}. "
                    "This means a job/manager process was forked from the "
                    "live server process instead of spawned cleanly."
                )
            finally:
                await queue.stop()
        finally:
            server_handle.server.close()
            await server_handle.server.wait_closed()

    asyncio.run(runner())
