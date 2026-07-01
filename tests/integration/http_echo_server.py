"""
Minimal asyncio HTTP/1.1 echo server used to reproduce the casmgr incident.

The production bug: casmgr runs a FastAPI/hypercorn asyncio server. queuemgr
forked that live process to run queue jobs. The forked child inherited the
listening socket's file descriptor and the parent's already-accepted
connection fds / epoll state. After the fork+fallback dance, the *parent's*
HTTP body parsing degraded permanently (every POST body became an empty
dict).

This module provides a tiny hand-rolled HTTP/1.1 server (asyncio.start_server)
that is good enough to prove the same class of bug: it reads a request line,
headers, a Content-Length body, and echoes the body back verbatim as the
response body. If a forked child ever touches the shared listener/accepted
socket state in a way that corrupts the parent, an echo will fail to match,
hang, or a connection will misbehave.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import asyncio
import socket
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class EchoServerHandle:
    """Handle to a running echo server."""

    server: asyncio.base_events.Server
    host: str
    port: int
    listen_socket: socket.socket
    request_count: int = 0


async def _handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    """
    Serve HTTP/1.1 keep-alive requests on a single connection, echoing body.

    Args:
        reader: Stream reader for the accepted connection.
        writer: Stream writer for the accepted connection.
    """
    try:
        while True:
            request_line = await reader.readline()
            if not request_line:
                break  # Connection closed by peer.

            headers: List[bytes] = []
            content_length = 0
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                headers.append(line)
                lower = line.lower()
                if lower.startswith(b"content-length:"):
                    try:
                        content_length = int(line.split(b":", 1)[1].strip())
                    except ValueError:
                        content_length = 0

            body = b""
            if content_length > 0:
                body = await reader.readexactly(content_length)

            response_body = body
            response = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: application/octet-stream\r\n"
                b"Content-Length: " + str(len(response_body)).encode("ascii") + b"\r\n"
                b"Connection: keep-alive\r\n"
                b"\r\n" + response_body
            )
            writer.write(response)
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:  # pylint: disable=broad-except
            pass


async def start_echo_server(host: str = "127.0.0.1", port: int = 0) -> EchoServerHandle:
    """
    Start the echo server on an ephemeral port (by default).

    Args:
        host: Host/interface to bind.
        port: Port to bind, 0 for an OS-assigned ephemeral port.

    Returns:
        Handle with the running server, resolved host/port, and the raw
        listening socket (for fd/inode inspection by tests).
    """
    server = await asyncio.start_server(_handle_client, host=host, port=port)
    sockets = server.sockets
    assert sockets, "asyncio.start_server produced no listening sockets"
    listen_socket = sockets[0]
    bound_host, bound_port = listen_socket.getsockname()[:2]
    return EchoServerHandle(
        server=server,
        host=bound_host,
        port=bound_port,
        listen_socket=listen_socket,
    )


def send_http_post(
    sock: socket.socket, path: str, body: bytes, keep_alive: bool = True
) -> bytes:
    """
    Send a single HTTP/1.1 POST request on an already-connected blocking
    socket and return the response body.

    Args:
        sock: Connected, blocking TCP socket.
        path: Request path.
        body: Request body bytes.
        keep_alive: Whether to request Connection: keep-alive.

    Returns:
        The response body bytes, as echoed by the server.
    """
    connection_header = b"keep-alive" if keep_alive else b"close"
    request = (
        b"POST " + path.encode("ascii") + b" HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
        b"Connection: " + connection_header + b"\r\n"
        b"\r\n" + body
    )
    sock.sendall(request)
    return _read_http_response_body(sock)


def _read_http_response_body(sock: socket.socket) -> bytes:
    """
    Read a single HTTP/1.1 response (status line + headers + body) from a
    blocking socket and return the body bytes.

    Args:
        sock: Connected, blocking TCP socket positioned at a response start.

    Returns:
        The response body bytes.
    """
    buf = b""
    # Read headers until CRLFCRLF.
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Connection closed while reading headers")
        buf += chunk

    header_blob, _, rest = buf.partition(b"\r\n\r\n")
    header_lines = header_blob.split(b"\r\n")
    content_length = 0
    for line in header_lines[1:]:
        lower = line.lower()
        if lower.startswith(b"content-length:"):
            content_length = int(line.split(b":", 1)[1].strip())

    body = rest
    while len(body) < content_length:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Connection closed while reading body")
        body += chunk

    return body[:content_length]
