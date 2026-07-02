#!/usr/bin/env python3
"""Deep functional verification of svo-chunker: chunking commands, the file
transfer protocol, the background queue, and the svo-client library.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com

This complements ``verify_prod_deploy.py`` (which only checks
health/version/queue-fallback across the whole fleet) and
``verify_embed_deep.py`` (embedding service internals). It focuses
specifically on svo-chunker's application behavior:

  1. ``chunk`` / ``file`` / ``file_chunk`` / ``stanza_runtime`` commands with
     real text, verifying chunk boundaries, metadata, and (when Stanza/SV is
     enabled) semantic scoring -- and confirming the svo -> embed integration
     actually produces embeddings.
  2. The file transfer protocol end to end: ``transfer_upload_begin`` -> PUT
     chunk(s) -> ``transfer_upload_complete`` -> ``file_chunk`` by
     ``transfer_id`` -> chunked download of the (large) NDJSON result via
     ``transfer_download_begin``/``.../chunks``, with SHA-256 verification at
     every step. Exercised at two sizes: a small file (a few KB) and a large
     one (default 300 KB, configurable) that forces multi-offset download.
  3. The background queue: every chunk/file_chunk call on this deployment is
     async-only (``chunk`` returns a ``job_id`` immediately; the actual
     result is fetched via ``queue_get_job_status``). This script polls to
     completion, checks ``queue_get_job_logs``, runs several jobs back to
     back and several in parallel, and confirms ``queue_health`` and the
     command API are still alive before/after.
  4. svo-client (PyPI package) against the live server: health, ``chunk()``,
     and ``file_chunk()`` (base64 channel, which itself uses the transfer
     protocol for the result), via a caller-provided Python interpreter
     (e.g. a scratch venv with ``pip install svo-client==<version>``) since
     this script itself only depends on ``requests``.

Design notes
------------
* Connectivity: at write time (2026-07-02) port 8009 (svo-chunker) is
  reachable directly from this workstation over HTTPS; no auth/mTLS is
  enforced on JSON-RPC or transfer endpoints for this deployment. If that
  ever changes (firewalled, as an older note in verify_prod_deploy.py
  claims), use ``--ssh-tunnel`` to open ``ssh -L <local>:127.0.0.1:8009 -N``
  to the target host for the duration of the run; the tunnel is always torn
  down on exit (including on error/Ctrl-C).
* TLS: internal PKI, not publicly trusted -- verification is disabled
  (equivalent to curl -k), matching sibling scripts in this directory.
* Idempotent: every run only adds a few short-lived queue jobs and transfer
  sessions (server-side TTL cleanup applies); no destructive calls.
* Slow operations (chunk/file_chunk on non-trivial text) go through the
  queue and can take tens of seconds to a few minutes on this deployment
  (one embedding-service round trip per chunk); this script polls actively
  with bounded timeouts rather than sleeping blindly.

Usage
-----
    python3 scripts/verify_svo_deep.py
    python3 scripts/verify_svo_deep.py --host 192.168.254.26 --port 8009
    python3 scripts/verify_svo_deep.py --ssh-tunnel --ssh root@192.168.254.26
    python3 scripts/verify_svo_deep.py --svo-client-python /path/to/venv/bin/python
    python3 scripts/verify_svo_deep.py --big-file-bytes 1000000 --json report.json
    python3 scripts/verify_svo_deep.py --skip-parallel --skip-svo-client
"""

from __future__ import annotations

import argparse
import atexit
import base64
import hashlib
import json
import shlex
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:  # pragma: no cover
    print("ERROR: the 'requests' package is required (pip install requests)", file=sys.stderr)
    sys.exit(2)


DEFAULT_HOST = "192.168.254.26"
DEFAULT_PORT = 8009
DEFAULT_SSH_TARGET = "root@192.168.254.26"
DEFAULT_TUNNEL_LOCAL_PORT = 18009

REPO_ROOT = Path(__file__).resolve().parents[1]
SVO_README = Path("/home/vasilyvz/projects/tools/svo/README.md")
SVO_CLIENT_README = Path("/home/vasilyvz/projects/tools/svo_client/README.md")


# --------------------------------------------------------------------------
# Result plumbing (same shape as verify_prod_deploy.py / verify_embed_deep.py)
# --------------------------------------------------------------------------

@dataclass
class CheckResult:
    name: str
    status: str  # "PASS" | "FAIL" | "WAITING" | "SKIP"
    detail: str = ""


@dataclass
class SectionReport:
    section: str
    checks: List[CheckResult] = field(default_factory=list)

    def add(self, name: str, status: str, detail: str = "") -> CheckResult:
        cr = CheckResult(name=name, status=status, detail=detail)
        self.checks.append(cr)
        return cr

    @property
    def overall(self) -> str:
        statuses = {c.status for c in self.checks}
        if "FAIL" in statuses:
            return "FAIL"
        if "WAITING" in statuses:
            return "WAITING"
        if not statuses or statuses == {"SKIP"}:
            return "SKIP"
        return "PASS"


# --------------------------------------------------------------------------
# SSH tunnel (optional)
# --------------------------------------------------------------------------

class SshTunnel:
    """Manages ``ssh -L local:127.0.0.1:remote -N`` for the run's lifetime."""

    def __init__(self, ssh_target: str, local_port: int, remote_port: int):
        self.ssh_target = ssh_target
        self.local_port = local_port
        self.remote_port = remote_port
        self.proc: Optional[subprocess.Popen] = None

    def start(self) -> None:
        cmd = [
            "ssh",
            "-o", "ConnectTimeout=8",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ExitOnForwardFailure=yes",
            "-L", f"{self.local_port}:127.0.0.1:{self.remote_port}",
            "-N",
            self.ssh_target,
        ]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        atexit.register(self.stop)
        # Wait for the tunnel to come up.
        for _ in range(50):
            if self.proc.poll() is not None:
                _, err = self.proc.communicate(timeout=1)
                raise RuntimeError(f"ssh tunnel exited early: {err.decode(errors='replace')[:300]}")
            try:
                s = requests.get(f"https://127.0.0.1:{self.local_port}/health", timeout=0.5, verify=False)
                if s.status_code:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.2)
        raise RuntimeError("ssh tunnel did not come up within timeout")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.proc = None


# --------------------------------------------------------------------------
# HTTP / JSON-RPC helpers
# --------------------------------------------------------------------------

def http_get(url: str, timeout: float = 10.0, **kwargs) -> Tuple[Optional[int], Any, Optional[str]]:
    try:
        resp = requests.get(url, timeout=timeout, verify=False, **kwargs)
        try:
            body: Any = resp.json()
        except ValueError:
            body = resp.content
        return resp.status_code, body, None
    except requests.RequestException as exc:
        return None, None, str(exc)


def http_post_json(url: str, payload: Dict[str, Any], timeout: float = 30.0) -> Tuple[Optional[int], Any, Optional[str]]:
    try:
        resp = requests.post(url, json=payload, timeout=timeout, verify=False)
        try:
            body: Any = resp.json()
        except ValueError:
            body = resp.text
        return resp.status_code, body, None
    except requests.RequestException as exc:
        return None, None, str(exc)


def http_put_bytes(url: str, data: bytes, timeout: float = 60.0) -> Tuple[Optional[int], Any, Optional[str]]:
    try:
        resp = requests.put(url, data=data, timeout=timeout, verify=False,
                             headers={"Content-Type": "application/octet-stream"})
        try:
            body: Any = resp.json()
        except ValueError:
            body = resp.text
        return resp.status_code, body, None
    except requests.RequestException as exc:
        return None, None, str(exc)


class SvoRpc:
    """Thin JSON-RPC + transfer-protocol client for svo-chunker."""

    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._id = 0

    def call(self, method: str, params: Optional[Dict[str, Any]] = None,
              timeout: Optional[float] = None) -> Tuple[Optional[int], Any, Optional[str]]:
        self._id += 1
        payload = {"jsonrpc": "2.0", "method": method, "params": params or {}, "id": self._id}
        return http_post_json(f"{self.base_url}/api/jsonrpc", payload, timeout=timeout or self.timeout)

    def health(self) -> Tuple[Optional[int], Any, Optional[str]]:
        return http_get(f"{self.base_url}/health", timeout=self.timeout)

    def queue_health(self):
        return self.call("queue_health", {})

    def submit_job(self, method: str, params: Dict[str, Any]) -> Tuple[Optional[str], Any]:
        """Submit an async command; returns (job_id, raw_response)."""
        status, body, err = self.call(method, params)
        if err or status != 200 or not isinstance(body, dict):
            return None, {"status": status, "body": body, "err": err}
        result = body.get("result")
        if not isinstance(result, dict) or not result.get("success"):
            return None, body
        return result.get("job_id"), body

    def poll_job(self, job_id: str, max_wait: float = 180.0, interval: float = 2.0) -> Tuple[str, Dict[str, Any], float]:
        """Poll queue_get_job_status until terminal. Returns (status, data, elapsed_sec)."""
        t0 = time.monotonic()
        last_data: Dict[str, Any] = {}
        while (time.monotonic() - t0) < max_wait:
            status, body, err = self.call("queue_get_job_status", {"job_id": job_id}, timeout=15.0)
            if not err and status == 200 and isinstance(body, dict):
                result = body.get("result")
                if isinstance(result, dict) and isinstance(result.get("data"), dict):
                    last_data = result["data"]
                    st = last_data.get("status", "")
                    if st in ("completed", "failed", "cancelled", "error"):
                        return st, last_data, time.monotonic() - t0
            time.sleep(interval)
        return "timeout", last_data, time.monotonic() - t0

    def job_logs(self, job_id: str) -> Tuple[Optional[int], Any, Optional[str]]:
        return self.call("queue_get_job_logs", {"job_id": job_id})

    def upload_file(self, filename: str, raw: bytes, timeout: float = 60.0) -> Tuple[Optional[str], Optional[str]]:
        """Full upload cycle (begin -> PUT -> complete). Returns (transfer_id, error)."""
        sha = hashlib.sha256(raw).hexdigest()
        status, body, err = self.call("transfer_upload_begin", {
            "filename": filename, "size_bytes": len(raw),
            "checksum_value": sha, "compression": "identity",
        }, timeout=timeout)
        if err or status != 200:
            return None, f"transfer_upload_begin failed: status={status} err={err} body={body}"
        result = body.get("result") if isinstance(body, dict) else None
        data = result.get("data") if isinstance(result, dict) else None
        if not isinstance(data, dict) or not data.get("transfer_id"):
            return None, f"transfer_upload_begin: unexpected shape: {body}"
        transfer_id = data["transfer_id"]

        put_url = f"{self.base_url}/api/transfer/uploads/{transfer_id}/chunks?offset=0"
        pstatus, pbody, perr = http_put_bytes(put_url, raw, timeout=timeout)
        if perr or pstatus not in (200, 201, 204):
            return None, f"PUT chunk failed: status={pstatus} err={perr} body={pbody}"

        status, body, err = self.call("transfer_upload_complete", {"transfer_id": transfer_id}, timeout=timeout)
        if err or status != 200:
            return None, f"transfer_upload_complete failed: status={status} err={err} body={body}"
        result = body.get("result") if isinstance(body, dict) else None
        data = result.get("data") if isinstance(result, dict) else None
        if not isinstance(data, dict) or not data.get("completed"):
            return None, f"transfer_upload_complete: not completed: {body}"
        actual_sha = data.get("checksum_value")
        if actual_sha != sha:
            return None, f"checksum mismatch after upload: expected={sha} got={actual_sha}"
        return transfer_id, None

    def download_transfer(self, transfer_id: str, timeout: float = 60.0) -> Tuple[Optional[bytes], Optional[str]]:
        """Fetch a completed download transfer's bytes, verifying checksum."""
        status, body, err = self.call("transfer_download_status", {"transfer_id": transfer_id}, timeout=timeout)
        if err or status != 200:
            return None, f"transfer_download_status failed: status={status} err={err} body={body}"
        result = body.get("result") if isinstance(body, dict) else None
        data = result.get("data") if isinstance(result, dict) else None
        if not isinstance(data, dict):
            return None, f"transfer_download_status: unexpected shape: {body}"
        size_bytes = data.get("size_bytes", 0)
        expected_sha = data.get("checksum_value")
        limit = data.get("chunk_size", 1048576)

        chunks: List[bytes] = []
        offset = 0
        while offset < size_bytes:
            url = f"{self.base_url}/api/transfer/downloads/{transfer_id}/chunks?offset={offset}&limit={limit}"
            try:
                resp = requests.get(url, timeout=timeout, verify=False)
            except requests.RequestException as exc:
                return None, f"download chunk GET failed at offset={offset}: {exc}"
            if resp.status_code != 200:
                return None, f"download chunk GET status={resp.status_code} at offset={offset}"
            chunks.append(resp.content)
            offset += len(resp.content)
            if len(resp.content) == 0:
                break
        raw = b"".join(chunks)
        actual_sha = hashlib.sha256(raw).hexdigest()
        if expected_sha and actual_sha != expected_sha:
            return None, f"download checksum mismatch: expected={expected_sha} got={actual_sha} (size={len(raw)} vs {size_bytes})"
        return raw, None


def extract_chunks_from_command_result(cmd_result: Dict[str, Any], rpc: SvoRpc) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """Given the inner ``result`` of a completed chunk/file_chunk job, return
    the list of chunk dicts, resolving the transfer_id download if the
    payload wasn't inlined (small texts are inlined; larger ones use a
    download transfer -- see ``data.transfer_id`` / ``mime``).
    """
    if not cmd_result.get("success"):
        return None, f"command failed: {cmd_result.get('error')}"
    data = cmd_result.get("data") or {}
    transfer_id = data.get("transfer_id") or cmd_result.get("transfer_id")
    body = data.get("body") or cmd_result.get("body") or ""
    if body:
        lines = [line for line in body.splitlines() if line.strip()]
        try:
            return [json.loads(line) for line in lines], None
        except json.JSONDecodeError as exc:
            return None, f"inline body NDJSON parse error: {exc}"
    if transfer_id:
        raw, err = rpc.download_transfer(transfer_id)
        if err:
            return None, err
        lines = [line for line in raw.decode("utf-8", errors="replace").splitlines() if line.strip()]
        try:
            return [json.loads(line) for line in lines], None
        except json.JSONDecodeError as exc:
            return None, f"downloaded NDJSON parse error: {exc}"
    return None, "no inline body and no transfer_id in result"


# --------------------------------------------------------------------------
# Section 1: command inventory
# --------------------------------------------------------------------------

BUILTIN_QUEUE_COMMANDS = {
    "echo", "long_task", "job_status",
    "queue_add_job", "queue_start_job", "queue_stop_job", "queue_delete_job",
    "queue_get_job_status", "queue_get_job_logs", "queue_list_jobs", "queue_health",
    "help", "health", "config", "reload", "settings", "load", "unload", "plugins",
    "transport_management", "proxy_registration", "roletest",
    "transfer_upload_begin", "transfer_upload_status", "transfer_upload_complete",
    "transfer_download_begin", "transfer_download_status", "info",
}


def check_command_inventory(rpc: SvoRpc) -> SectionReport:
    r = SectionReport(section="command_inventory")
    status, body, err = http_get(f"{rpc.base_url}/commands", timeout=10.0)
    if err or status != 200 or not isinstance(body, dict):
        r.add("fetch_commands", "FAIL", f"status={status} err={err}")
        return r
    commands = body.get("commands")
    names = sorted(commands.keys()) if isinstance(commands, dict) else sorted(commands) if isinstance(commands, list) else []
    if not names:
        r.add("fetch_commands", "FAIL", "empty/unexpected /commands shape")
        return r
    app_commands = [n for n in names if n not in BUILTIN_QUEUE_COMMANDS]
    r.add("fetch_commands", "PASS", f"{len(names)} total, {len(app_commands)} application-specific")
    r.add("application_commands", "PASS" if app_commands else "FAIL", ", ".join(app_commands))
    expected_chunking = {"chunk", "file", "file_chunk", "stanza_runtime"}
    missing = expected_chunking - set(names)
    r.add("expected_chunking_commands_present", "PASS" if not missing else "FAIL",
          "all present" if not missing else f"missing: {missing}")
    return r


# --------------------------------------------------------------------------
# Section 2: chunking commands (chunk / file / file_chunk / stanza_runtime)
# --------------------------------------------------------------------------

def _load_sample_text(max_bytes: int) -> str:
    parts = []
    if SVO_README.is_file():
        parts.append(SVO_README.read_text(encoding="utf-8", errors="replace"))
    if SVO_CLIENT_README.is_file():
        parts.append(SVO_CLIENT_README.read_text(encoding="utf-8", errors="replace"))
    text = "\n\n".join(parts) if parts else (
        "SVO semantic chunker verification sample text. " * 200
    )
    raw = text.encode("utf-8")
    if len(raw) > max_bytes:
        raw = raw[:max_bytes]
    return raw.decode("utf-8", errors="ignore")


def check_stanza_runtime(rpc: SvoRpc, job_wait: float) -> Tuple[SectionReport, bool]:
    """Returns (report, sv_enabled)."""
    r = SectionReport(section="stanza_runtime")
    job_id, raw = rpc.submit_job("stanza_runtime", {"action": "status"})
    if not job_id:
        r.add("submit", "FAIL", f"no job_id: {raw}")
        return r, False
    status, data, elapsed = rpc.poll_job(job_id, max_wait=job_wait)
    if status != "completed":
        r.add("poll", "FAIL", f"status={status} after {elapsed:.1f}s")
        return r, False
    inner = data.get("result", {}).get("result", {})
    payload = inner.get("data", {}) if isinstance(inner, dict) else {}
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    sv_enabled = bool(payload.get("enabled_effective"))
    r.add("status", "PASS", f"enabled_effective={sv_enabled} state={payload.get('state')} "
                             f"loaded_languages={payload.get('loaded_languages')} ({elapsed:.1f}s)")
    return r, sv_enabled


def check_chunk_command(rpc: SvoRpc, text: str, use_sv: bool, job_wait: float, label: str) -> SectionReport:
    r = SectionReport(section=f"chunk[{label}]")
    job_id, raw = rpc.submit_job("chunk", {
        "texts": [text], "window": 3, "use_sv": use_sv, "language": "en", "type": "DocBlock",
    })
    if not job_id:
        r.add("submit", "FAIL", f"no job_id: {raw}")
        return r
    r.add("submit", "PASS", f"job_id={job_id}")
    status, data, elapsed = rpc.poll_job(job_id, max_wait=job_wait)
    if status != "completed":
        r.add("poll", "FAIL", f"status={status} after {elapsed:.1f}s data={str(data)[:300]}")
        return r
    cmd_result = data.get("result", {}).get("result", {})
    if not isinstance(cmd_result, dict):
        r.add("poll", "FAIL", f"unexpected result shape: {data}")
        return r
    if use_sv and not cmd_result.get("success"):
        # Expected failure mode when Stanza is disabled on the deployment.
        err = cmd_result.get("error", {})
        if err.get("data", {}).get("error_type") == "stanza_disabled":
            r.add("poll", "PASS", f"completed in {elapsed:.1f}s, use_sv correctly rejected (stanza_disabled)")
            return r
    chunks, err = extract_chunks_from_command_result(cmd_result, rpc)
    if err:
        r.add("poll", "FAIL", f"completed in {elapsed:.1f}s but: {err}")
        return r
    r.add("poll", "PASS", f"completed in {elapsed:.1f}s, {len(chunks)} chunks")

    # Structural sanity: ordinals monotonic, non-empty bodies, checksum-clean.
    ordinals = [c.get("ordinal") for c in chunks]
    monotonic = ordinals == sorted(ordinals)
    r.add("chunk_ordinals_monotonic", "PASS" if monotonic else "FAIL", f"range={ordinals[:1]}..{ordinals[-1:]}")
    nonempty = all((c.get("body") or "").strip() for c in chunks)
    r.add("chunk_bodies_nonempty", "PASS" if nonempty else "FAIL", "")
    reconstructed_len = sum(len(c.get("body", "")) for c in chunks)
    r.add("chunk_coverage_roughly_matches_input", "PASS" if reconstructed_len > 0.5 * len(text) else "FAIL",
          f"sum(body_len)={reconstructed_len} input_len={len(text)}")
    with_embedding = sum(1 for c in chunks if isinstance(c.get("embedding"), list) and len(c["embedding"]) > 0)
    r.add("embeddings_present (svo->embed integration)", "PASS" if with_embedding == len(chunks) else "FAIL",
          f"{with_embedding}/{len(chunks)} chunks have non-empty embedding "
          f"(model={chunks[0].get('embedding_model') if chunks else None})")
    return r


def check_file_command(rpc: SvoRpc, job_wait: float) -> SectionReport:
    r = SectionReport(section="file (preprocess)")
    if not SVO_README.is_file():
        r.add("submit", "SKIP", f"sample not found: {SVO_README}")
        return r
    raw = SVO_README.read_bytes()
    b64 = base64.b64encode(raw).decode("ascii")
    job_id, resp = rpc.submit_job("file", {"filename": "README.md", "file_content_base64": b64})
    if not job_id:
        r.add("submit", "FAIL", f"no job_id: {resp}")
        return r
    status, data, elapsed = rpc.poll_job(job_id, max_wait=job_wait)
    if status != "completed":
        r.add("poll", "FAIL", f"status={status} after {elapsed:.1f}s")
        return r
    cmd_result = data.get("result", {}).get("result", {})
    payload = cmd_result.get("data", {}) if isinstance(cmd_result, dict) else {}
    texts = payload.get("texts") if isinstance(payload, dict) else None
    ok = isinstance(texts, list) and len(texts) > 0 and sum(len(t) for t in texts) > 0
    r.add("poll", "PASS" if ok else "FAIL",
          f"completed in {elapsed:.1f}s, parts={len(texts) if texts else 0}")
    return r


# --------------------------------------------------------------------------
# Section 3: file transfer protocol (upload -> file_chunk by transfer_id ->
# chunked download), at a small and a large size.
# --------------------------------------------------------------------------

def _build_big_text(target_bytes: int) -> str:
    base = ""
    if SVO_README.is_file():
        base += SVO_README.read_text(encoding="utf-8", errors="replace")
    if SVO_CLIENT_README.is_file():
        base += "\n\n" + SVO_CLIENT_README.read_text(encoding="utf-8", errors="replace")
    if not base:
        base = "SVO semantic chunker transfer protocol verification sample.\n"
    content = []
    i = 0
    total = 0
    while total < target_bytes:
        piece = f"\n\n<!-- section {i} -->\n\n" + base
        content.append(piece)
        total += len(piece.encode("utf-8"))
        i += 1
    text = "".join(content)
    raw = text.encode("utf-8")[:target_bytes]
    return raw.decode("utf-8", errors="ignore")


def check_transfer_cycle(rpc: SvoRpc, size_bytes: int, label: str, job_wait: float) -> SectionReport:
    r = SectionReport(section=f"transfer_cycle[{label}, {size_bytes}B]")
    text = _build_big_text(size_bytes)
    raw = text.encode("utf-8")

    t_upload = time.perf_counter()
    transfer_id, err = rpc.upload_file(f"verify_{label}.md", raw, timeout=120.0)
    upload_elapsed = time.perf_counter() - t_upload
    if err:
        r.add("upload_cycle", "FAIL", err)
        return r
    r.add("upload_cycle", "PASS", f"transfer_id={transfer_id} {len(raw)}B in {upload_elapsed:.2f}s "
                                    f"({len(raw) / max(upload_elapsed, 1e-6) / 1024:.0f} KB/s)")

    job_id, resp = rpc.submit_job("file_chunk", {
        "filename": f"verify_{label}.md", "transfer_id": transfer_id,
        "window": 3, "use_sv": False, "language": "en",
    })
    if not job_id:
        r.add("file_chunk_submit", "FAIL", f"no job_id: {resp}")
        return r
    t_chunk = time.perf_counter()
    status, data, elapsed = rpc.poll_job(job_id, max_wait=job_wait)
    if status != "completed":
        r.add("file_chunk_poll", "FAIL", f"status={status} after {elapsed:.1f}s")
        return r
    cmd_result = data.get("result", {}).get("result", {})
    chunks, cerr = extract_chunks_from_command_result(cmd_result, rpc)
    chunk_elapsed = time.perf_counter() - t_chunk
    if cerr:
        r.add("file_chunk_poll", "FAIL", f"completed in {chunk_elapsed:.1f}s but: {cerr}")
        return r
    r.add("file_chunk_poll", "PASS", f"completed in {chunk_elapsed:.1f}s, {len(chunks)} chunks "
                                       f"(download+verify included)")

    ordinals = [c.get("ordinal") for c in chunks]
    monotonic = ordinals == sorted(ordinals)
    r.add("result_ordinals_monotonic", "PASS" if monotonic else "FAIL", f"{len(ordinals)} chunks")
    return r


# --------------------------------------------------------------------------
# Section 4: queue behavior -- sequential jobs, parallel jobs, health before/after
# --------------------------------------------------------------------------

def check_queue_health(rpc: SvoRpc, label: str) -> CheckResult:
    status, body, err = rpc.queue_health()
    if err or status != 200:
        return CheckResult(f"queue_health[{label}]", "FAIL", f"status={status} err={err}")
    result = body.get("result") if isinstance(body, dict) else None
    data = result.get("data") if isinstance(result, dict) else None
    if not isinstance(data, dict) or data.get("status") != "healthy":
        return CheckResult(f"queue_health[{label}]", "FAIL", f"unexpected: {body}")
    return CheckResult(f"queue_health[{label}]", "PASS",
                        f"running={data.get('running')} total={data.get('total_jobs')} "
                        f"failed={data.get('failed_jobs')} max_concurrent={data.get('max_concurrent_jobs')}")


def check_command_api_alive(rpc: SvoRpc) -> CheckResult:
    status, body, err = http_get(f"{rpc.base_url}/health", timeout=10.0)
    if err or status != 200 or not isinstance(body, dict) or body.get("status") != "ok":
        return CheckResult("command_api_alive", "FAIL", f"status={status} err={err} body={str(body)[:200]}")
    return CheckResult("command_api_alive", "PASS", f"uptime={body.get('uptime')}")


def check_queue_sequential_and_parallel(rpc: SvoRpc, job_wait: float) -> SectionReport:
    r = SectionReport(section="queue_sequential_and_parallel")
    r.checks.append(check_queue_health(rpc, "before"))

    # Sequential: 3 small jobs, one after another.
    seq_texts = [
        "Sequential queue test one: verifying jobs complete in order without state leakage.",
        "Sequential queue test two: a second short passage to chunk right after the first.",
        "Sequential queue test three: a third short passage confirming queue draining works.",
    ]
    seq_ok = True
    seq_timings = []
    for i, t in enumerate(seq_texts):
        job_id, resp = rpc.submit_job("chunk", {"texts": [t], "window": 3, "use_sv": False})
        if not job_id:
            seq_ok = False
            r.add(f"sequential_job_{i}", "FAIL", f"no job_id: {resp}")
            continue
        status, data, elapsed = rpc.poll_job(job_id, max_wait=job_wait)
        seq_timings.append(elapsed)
        cmd_result = data.get("result", {}).get("result", {}) if status == "completed" else {}
        ok = status == "completed" and cmd_result.get("success")
        seq_ok = seq_ok and ok
        r.add(f"sequential_job_{i}", "PASS" if ok else "FAIL", f"status={status} elapsed={elapsed:.1f}s")
    r.add("sequential_summary", "PASS" if seq_ok else "FAIL",
          f"{len(seq_texts)} jobs, timings={[f'{t:.1f}s' for t in seq_timings]}")

    # Parallel: 5 jobs submitted back-to-back, polled concurrently by round-robin.
    par_texts = [
        "Parallel job A checks concurrent submission does not corrupt job identifiers.",
        "Параллельное задание Б проверяет обработку кириллического текста при нагрузке.",
        "Parallel job C verifies isolation between simultaneously running chunk requests.",
        "Параллельное задание Г одновременно с другими не должно терять данные очереди.",
        "Parallel job E is the last of five concurrent submissions in this batch test.",
    ]
    job_ids: List[Optional[str]] = []
    t_submit = time.perf_counter()
    for t in par_texts:
        job_id, resp = rpc.submit_job("chunk", {"texts": [t], "window": 3, "use_sv": False})
        job_ids.append(job_id)
    submit_elapsed = time.perf_counter() - t_submit
    n_submitted = sum(1 for j in job_ids if j)
    r.add("parallel_submit", "PASS" if n_submitted == len(par_texts) else "FAIL",
          f"{n_submitted}/{len(par_texts)} accepted in {submit_elapsed:.2f}s, "
          f"unique_ids={len(set(j for j in job_ids if j)) == n_submitted}")

    t_poll = time.perf_counter()
    results: Dict[str, Tuple[str, Dict[str, Any]]] = {}
    pending = [j for j in job_ids if j]
    while pending and (time.perf_counter() - t_poll) < job_wait:
        still_pending = []
        for job_id in pending:
            status, body, err = rpc.call("queue_get_job_status", {"job_id": job_id}, timeout=15.0)
            data = {}
            if not err and status == 200 and isinstance(body, dict):
                result = body.get("result")
                if isinstance(result, dict) and isinstance(result.get("data"), dict):
                    data = result["data"]
            st = data.get("status", "")
            if st in ("completed", "failed", "cancelled", "error"):
                results[job_id] = (st, data)
            else:
                still_pending.append(job_id)
        pending = still_pending
        if pending:
            time.sleep(1.5)
    poll_elapsed = time.perf_counter() - t_poll

    n_completed = sum(1 for st, _ in results.values() if st == "completed")
    n_correct = 0
    for job_id, (st, data) in results.items():
        if st != "completed":
            continue
        cmd_result = data.get("result", {}).get("result", {})
        if isinstance(cmd_result, dict) and cmd_result.get("success"):
            n_correct += 1
    r.add("parallel_poll", "PASS" if n_completed == len(par_texts) and n_correct == len(par_texts) else "FAIL",
          f"{n_completed}/{len(par_texts)} completed, {n_correct}/{len(par_texts)} succeeded, "
          f"{len(pending)} still pending, elapsed={poll_elapsed:.1f}s")

    r.checks.append(check_queue_health(rpc, "after"))
    r.checks.append(check_command_api_alive(rpc))
    return r


# --------------------------------------------------------------------------
# Section 5: svo-client library (via caller-provided interpreter)
# --------------------------------------------------------------------------

SVO_CLIENT_SMOKE_SCRIPT = textwrap.dedent('''\
    import asyncio, base64, json, sys, time

    async def main():
        from svo_client.client import SvoChunkerClient, build_file_chunk_params

        host, port = sys.argv[1], int(sys.argv[2])
        text = open(sys.argv[3], encoding="utf-8", errors="replace").read()[:6000]
        out: dict = {}
        async with SvoChunkerClient(protocol="https", host=host, port=port,
                                     check_hostname=False, timeout=240.0) as client:
            h = await client.health()
            out["health"] = h.get("status") if isinstance(h, dict) else str(h)

            t0 = time.perf_counter()
            chunks = await client.chunk(text, language="en", use_sv=False, chunking_version="1.0")
            out["chunk_text"] = {
                "n_chunks": len(chunks),
                "elapsed_sec": round(time.perf_counter() - t0, 2),
                "first_body": (chunks[0].body[:60] if chunks else ""),
            }

            raw = text.encode("utf-8")
            b64 = base64.b64encode(raw).decode()
            params = build_file_chunk_params(
                filename="verify_sample.md", file_content_base64=b64, filter_name=None,
                window=3, language="en", use_sv=False, chunking_version="1.0",
            )
            t0 = time.perf_counter()
            data = await client.file_chunk(params, download_path=sys.argv[4])
            import os
            out["file_chunk"] = {
                "transfer_id": getattr(data, "transfer_id", None),
                "elapsed_sec": round(time.perf_counter() - t0, 2),
                "downloaded_bytes": os.path.getsize(sys.argv[4]) if os.path.exists(sys.argv[4]) else 0,
            }
        print(json.dumps({"ok": True, **out}))

    asyncio.run(main())
''')


def check_svo_client(python_bin: Optional[str], host: str, local_port: int, job_wait: float) -> SectionReport:
    r = SectionReport(section="svo_client_library")
    if not python_bin:
        r.add("smoke_test", "SKIP", "no --svo-client-python given; pass a venv interpreter with svo-client installed")
        return r
    if not Path(python_bin).exists():
        r.add("smoke_test", "FAIL", f"interpreter not found: {python_bin}")
        return r

    check = subprocess.run([python_bin, "-c", "import svo_client; print(svo_client.__file__)"],
                            capture_output=True, text=True, timeout=30)
    if check.returncode != 0:
        r.add("import_svo_client", "FAIL", check.stderr.strip()[-400:])
        return r
    r.add("import_svo_client", "PASS", check.stdout.strip())

    with tempfile.TemporaryDirectory() as td:
        script_path = Path(td) / "smoke.py"
        script_path.write_text(SVO_CLIENT_SMOKE_SCRIPT, encoding="utf-8")
        sample_path = Path(td) / "sample.md"
        sample_text = _load_sample_text(6000)
        sample_path.write_text(sample_text, encoding="utf-8")
        out_path = Path(td) / "out.jsonl"

        proc = subprocess.run(
            [python_bin, str(script_path), host, str(local_port), str(sample_path), str(out_path)],
            capture_output=True, text=True, timeout=job_wait + 60,
        )
        if proc.returncode != 0:
            r.add("smoke_test", "FAIL", f"exit={proc.returncode} stderr_tail={proc.stderr.strip()[-500:]}")
            return r
        try:
            out_line = [line for line in proc.stdout.splitlines() if line.strip().startswith("{")][-1]
            out = json.loads(out_line)
        except (IndexError, json.JSONDecodeError) as exc:
            r.add("smoke_test", "FAIL", f"could not parse output: {exc}; stdout_tail={proc.stdout[-500:]}")
            return r

        r.add("health", "PASS" if out.get("health") == "ok" else "FAIL", str(out.get("health")))
        ct = out.get("chunk_text", {})
        r.add("chunk_text", "PASS" if ct.get("n_chunks", 0) > 0 else "FAIL",
              f"{ct.get('n_chunks')} chunks in {ct.get('elapsed_sec')}s, first={ct.get('first_body')!r}")
        fc = out.get("file_chunk", {})
        r.add("file_chunk", "PASS" if fc.get("downloaded_bytes", 0) > 0 else "FAIL",
              f"transfer_id={fc.get('transfer_id')} downloaded={fc.get('downloaded_bytes')}B "
              f"in {fc.get('elapsed_sec')}s")
    return r


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def print_report_table(reports: List[SectionReport]) -> None:
    col = max(len("SECTION"), max((len(r.section) for r in reports), default=0))
    header = f"{'SECTION'.ljust(col)}  STATUS"
    print(header)
    print("-" * (len(header) + 40))
    for r in reports:
        print(f"{r.section.ljust(col)}  {r.overall}")
        for c in r.checks:
            marker = {"PASS": "[PASS]", "FAIL": "[FAIL]", "WAITING": "[WAIT]", "SKIP": "[SKIP]"}[c.status]
            print(f"    {marker} {c.name}: {c.detail}")
    print()
    print("Summary:")
    for r in reports:
        print(f"  {r.section.ljust(col)}  {r.overall}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default=DEFAULT_HOST, help="svo-chunker host (default: %(default)s)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="svo-chunker port (default: %(default)s)")
    parser.add_argument("--ssh", default=DEFAULT_SSH_TARGET, help="ssh target for --ssh-tunnel (default: %(default)s)")
    parser.add_argument("--ssh-tunnel", action="store_true",
                         help="open ssh -L local:127.0.0.1:port -N to --ssh and talk to 127.0.0.1:local instead")
    parser.add_argument("--tunnel-local-port", type=int, default=DEFAULT_TUNNEL_LOCAL_PORT)
    parser.add_argument("--job-wait", type=float, default=240.0, help="max seconds to poll a single queue job (default: %(default)s)")
    parser.add_argument("--big-transfer-job-wait", type=float, default=600.0,
                         help="max seconds to poll the large-file file_chunk job specifically "
                              "(observed 267-360s for a 300KB/~2200-chunk file on this deployment, "
                              "since every chunk round-trips to the embedding service one at a time; "
                              "duration grows under concurrent queue load, so the default has extra "
                              "margin over the observed range; default: %(default)s)")
    parser.add_argument("--big-file-bytes", type=int, default=300_000, help="size of the large transfer-cycle test file (default: %(default)s)")
    parser.add_argument("--small-file-bytes", type=int, default=4_000, help="size of the small transfer-cycle test file (default: %(default)s)")
    parser.add_argument("--svo-client-python", default=None,
                         help="path to a Python interpreter with `pip install svo-client==<version>` for section 5 "
                              "(e.g. a scratch venv); omit to skip that section")
    parser.add_argument("--skip-parallel", action="store_true", help="skip the 5-parallel-jobs queue test")
    parser.add_argument("--skip-svo-client", action="store_true", help="skip the svo-client library section entirely")
    parser.add_argument("--skip-big-transfer", action="store_true", help="skip the large-file transfer cycle (keeps the small one)")
    parser.add_argument("--json", default=None, help="also write a JSON report to this path")
    args = parser.parse_args()

    tunnel: Optional[SshTunnel] = None
    effective_host = args.host
    effective_port = args.port
    if args.ssh_tunnel:
        tunnel = SshTunnel(args.ssh, args.tunnel_local_port, args.port)
        try:
            print(f"[*] opening ssh tunnel {args.tunnel_local_port} -> {args.ssh}:127.0.0.1:{args.port} ...")
            tunnel.start()
        except Exception as exc:
            print(f"ERROR: could not establish ssh tunnel: {exc}", file=sys.stderr)
            return 2
        effective_host = "127.0.0.1"
        effective_port = args.tunnel_local_port

        def _cleanup(*_a):
            tunnel.stop()

        signal.signal(signal.SIGINT, lambda *_a: (_cleanup(), sys.exit(130)))
        signal.signal(signal.SIGTERM, lambda *_a: (_cleanup(), sys.exit(143)))

    base_url = f"https://{effective_host}:{effective_port}"
    rpc = SvoRpc(base_url)

    reports: List[SectionReport] = []
    try:
        # 0. connectivity sanity before anything else.
        conn = SectionReport(section="connectivity")
        status, body, err = rpc.health()
        if err or status != 200:
            conn.add("health", "FAIL", f"status={status} err={err}")
            reports.append(conn)
            print_report_table(reports)
            print(f"\nERROR: cannot reach {base_url}/health; aborting further checks.", file=sys.stderr)
            return 2
        conn.add("health", "PASS", f"status={body.get('status')} version={body.get('version')}")
        reports.append(conn)

        # 1. command inventory
        reports.append(check_command_inventory(rpc))

        # 2. chunking commands
        sample = _load_sample_text(6000)
        stanza_report, sv_enabled = check_stanza_runtime(rpc, args.job_wait)
        reports.append(stanza_report)
        reports.append(check_chunk_command(rpc, sample, use_sv=False, job_wait=args.job_wait, label="use_sv=False"))
        reports.append(check_chunk_command(rpc, sample, use_sv=True, job_wait=args.job_wait,
                                            label="use_sv=True" + ("" if sv_enabled else " (expect stanza_disabled)")))
        reports.append(check_file_command(rpc, args.job_wait))

        # 3. transfer protocol
        reports.append(check_transfer_cycle(rpc, args.small_file_bytes, "small", args.job_wait))
        if not args.skip_big_transfer:
            reports.append(check_transfer_cycle(rpc, args.big_file_bytes, "big", args.big_transfer_job_wait))

        # 4. queue behavior
        if args.skip_parallel:
            r = SectionReport(section="queue_sequential_and_parallel")
            r.add("parallel_poll", "SKIP", "--skip-parallel")
            reports.append(r)
        else:
            reports.append(check_queue_sequential_and_parallel(rpc, args.job_wait))

        # 5. svo-client library
        if args.skip_svo_client:
            r = SectionReport(section="svo_client_library")
            r.add("smoke_test", "SKIP", "--skip-svo-client")
            reports.append(r)
        else:
            reports.append(check_svo_client(args.svo_client_python, effective_host, effective_port, args.job_wait))

    finally:
        if tunnel:
            print("[*] closing ssh tunnel ...")
            tunnel.stop()

    print_report_table(reports)

    if args.json:
        payload = {
            "host": args.host,
            "port": args.port,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "sections": [
                {"section": r.section, "overall": r.overall, "checks": [c.__dict__ for c in r.checks]}
                for r in reports
            ],
        }
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"\nJSON report written to: {args.json}")

    hard_fail = any(r.overall == "FAIL" for r in reports)
    return 1 if hard_fail else 0


if __name__ == "__main__":
    sys.exit(main())
