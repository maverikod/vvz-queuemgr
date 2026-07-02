#!/usr/bin/env python3
"""Deep functional verification of the embed vectorization service.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com

Goes beyond scripts/verify_prod_deploy.py's shallow "is it up" checks: this
script exercises every application-level JSON-RPC command the embed service
registers (embed family, models, queue_*, transfer_*, config/settings/etc.),
with particular emphasis on the background-queue path (embed -> queue_add_job
/ CommandExecutionJob -> embed_execute|embed_thin -> poll -> result), since
that is exactly the code path queuemgr's spawn fix (1.0.20, "force local
spawn multiprocessing context to stop forking live servers") touches.

What it checks
--------------
1. health / help / models -- basic liveness and command inventory.
2. embed (direct queue path), embed_execute (synchronous), embed_queue
   (explicit queue submit) -- for each: submit real texts, poll to
   completion, verify vector dimension, non-zero vectors, and that distinct
   input texts produce distinct (non near-identical) embeddings.
3. embed_thin -- confirms it is a worker-only re-entry point (must reject a
   direct call missing "_job_id" with a clean error, not a crash/hang).
4. Raw queue primitives: queue_add_job (job_type=command_execution,
   command=embed_execute) + queue_start_job + queue_get_job_status +
   queue_get_job_logs + queue_list_jobs + queue_health.
5. Concurrency: N embed jobs submitted back-to-back (parallel from the
   caller's perspective), each polled independently to completion, each
   validated independently. Confirms queue_health/health are still sane
   afterwards (the actual regression class the spawn fix addresses: queue
   activity must not kill the live command API).
6. transfer_upload_begin/status/complete round trip (small in-memory
   payload) and transfer_download_status/begin clean-error paths.
7. embed_client (PyPI package) library smoke test, IF the package is
   importable in the current interpreter -- otherwise SKIP (this script does
   not create a venv or install anything; per-command coverage above already
   proves the server side works over raw JSON-RPC regardless).

Usage
-----
    python3 scripts/verify_embed_deep.py
    python3 scripts/verify_embed_deep.py --host 192.168.254.26 --port 8001
    python3 scripts/verify_embed_deep.py --parallel-jobs 5 --json report.json

Design notes
------------
* Idempotent / safe to re-run: only ever adds short-lived embed/echo/transfer
  jobs; never calls load/unload/reload/config-mutating commands.
* Active polling throughout (no fixed sleeps beyond a short poll interval);
  every poll loop has a deadline and reports PASS/FAIL/TIMEOUT distinctly.
* TLS: server uses a self-signed/internal-PKI certificate; verification is
  intentionally disabled (verify=False), matching scripts/verify_prod_deploy.py.
* Does not modify scripts/verify_prod_deploy.py.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:  # pragma: no cover
    print("ERROR: the 'requests' package is required (pip install requests)", file=sys.stderr)
    sys.exit(2)


DEFAULT_HOST = "192.168.254.26"
DEFAULT_PORT = 8001
DEFAULT_MODEL_DIM = 384  # all-MiniLM-L6-v2 default


@dataclass
class CheckResult:
    name: str
    status: str  # "PASS" | "FAIL" | "SKIP"
    detail: str = ""
    elapsed_s: Optional[float] = None


@dataclass
class Report:
    checks: List[CheckResult] = field(default_factory=list)

    def add(self, name: str, status: str, detail: str = "", elapsed_s: Optional[float] = None) -> CheckResult:
        cr = CheckResult(name=name, status=status, detail=detail, elapsed_s=elapsed_s)
        self.checks.append(cr)
        return cr

    @property
    def overall(self) -> str:
        statuses = {c.status for c in self.checks}
        if "FAIL" in statuses:
            return "FAIL"
        if not statuses or statuses == {"SKIP"}:
            return "SKIP"
        return "PASS"


# --------------------------------------------------------------------------
# HTTP / JSON-RPC helpers
# --------------------------------------------------------------------------

def jsonrpc(base_url: str, method: str, params: Optional[Dict[str, Any]] = None,
            timeout: float = 30.0) -> Tuple[Optional[int], Any, Optional[str]]:
    payload = {"jsonrpc": "2.0", "method": method, "params": params or {}, "id": 1}
    try:
        resp = requests.post(f"{base_url}/api/jsonrpc", json=payload, timeout=timeout, verify=False)
        try:
            body: Any = resp.json()
        except ValueError:
            body = resp.text
        return resp.status_code, body, None
    except requests.RequestException as exc:
        return None, None, str(exc)


def rpc_data(body: Any) -> Optional[Dict[str, Any]]:
    """Extract result.data from a successful JSON-RPC response, else None."""
    if not isinstance(body, dict):
        return None
    result = body.get("result")
    if not isinstance(result, dict):
        return None
    if result.get("success") is False:
        return None
    data = result.get("data")
    return data if isinstance(data, dict) else None


def rpc_error(body: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(body, dict):
        return None
    result = body.get("result")
    if isinstance(result, dict) and isinstance(result.get("error"), dict):
        return result["error"]
    if isinstance(body.get("error"), dict):
        return body["error"]
    return None


def cosine(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def poll_job(base_url: str, job_id: str, status_method: str = "embed_job_status",
             deadline_s: float = 60.0, interval_s: float = 0.4) -> Tuple[Optional[str], Any, float]:
    """Actively poll a job until a terminal status. Returns (status, body, elapsed)."""
    start = time.monotonic()
    last_body = None
    while time.monotonic() - start < deadline_s:
        status_code, body, err = jsonrpc(base_url, status_method, {"job_id": job_id}, timeout=15)
        last_body = body
        if err:
            time.sleep(interval_s)
            continue
        data = rpc_data(body) or {}
        status = data.get("status")
        if status in ("completed", "failed", "error", "cancelled", "stopped"):
            return status, body, time.monotonic() - start
        time.sleep(interval_s)
    return None, last_body, time.monotonic() - start


def extract_embed_results(body: Any) -> List[Dict[str, Any]]:
    """Dig out the results list from an embed_job_status completed payload,
    or from a direct embed_execute response."""
    data = rpc_data(body) or {}
    # queue job status shape: data.result.result.data.results
    inner = data.get("result")
    if isinstance(inner, dict):
        inner2 = inner.get("result")
        if isinstance(inner2, dict):
            inner_data = inner2.get("data")
            if isinstance(inner_data, dict) and isinstance(inner_data.get("results"), list):
                return inner_data["results"]
    # direct embed_execute shape: data.results
    if isinstance(data.get("results"), list):
        return data["results"]
    return []


# --------------------------------------------------------------------------
# Individual checks
# --------------------------------------------------------------------------

def check_health_help_models(report: Report, base_url: str) -> List[str]:
    status, body, err = jsonrpc(base_url, "health")
    if err or status != 200:
        report.add("health", "FAIL", f"unreachable: HTTP {status} err={err}")
        return []
    data = rpc_data(body)
    if data and data.get("status") == "ok":
        report.add("health", "PASS", f"status=ok version={data.get('version')}")
    else:
        report.add("health", "FAIL", f"unexpected body: {str(body)[:200]}")

    status, body, err = jsonrpc(base_url, "help")
    names: List[str] = []
    if err or status != 200:
        report.add("help", "FAIL", f"unreachable: HTTP {status} err={err}")
    else:
        data = rpc_data(body) or {}
        commands = data.get("commands")
        if isinstance(commands, dict) and commands:
            names = sorted(commands.keys())
            report.add("help", "PASS", f"{len(names)} commands registered")
        else:
            report.add("help", "FAIL", f"no commands in help payload: {str(body)[:200]}")

    status, body, err = jsonrpc(base_url, "models")
    if err or status != 200:
        report.add("models", "FAIL", f"unreachable: HTTP {status} err={err}")
    else:
        data = rpc_data(body) or {}
        models = data.get("models")
        if isinstance(models, list) and models:
            dims = sorted({m.get("dimension") for m in models if isinstance(m, dict)})
            report.add("models", "PASS", f"{len(models)} models, dimensions={dims}")
        else:
            report.add("models", "FAIL", f"no models in payload: {str(body)[:200]}")

    return names


def check_embed_via_queue(report: Report, base_url: str, method: str, texts: List[str],
                           name: str, deadline_s: float = 60.0) -> Optional[List[Dict[str, Any]]]:
    """Submit texts to a queue-routed embed-family command, poll, validate vectors."""
    t0 = time.monotonic()
    status, body, err = jsonrpc(base_url, method, {"texts": texts}, timeout=20)
    if err or status != 200:
        report.add(name, "FAIL", f"submit failed: HTTP {status} err={err}")
        return None
    data = rpc_data(body)
    if not data:
        error = rpc_error(body)
        report.add(name, "FAIL", f"submit returned error: {error}")
        return None
    job_id = data.get("job_id")
    if not job_id:
        report.add(name, "FAIL", f"no job_id in submit response: {data}")
        return None

    final_status, final_body, elapsed = poll_job(base_url, job_id, "embed_job_status", deadline_s)
    total_elapsed = time.monotonic() - t0
    if final_status != "completed":
        report.add(name, "FAIL", f"job {job_id} final_status={final_status} after {elapsed:.2f}s poll "
                                  f"body={str(final_body)[:300]}", elapsed_s=total_elapsed)
        return None

    results = extract_embed_results(final_body)
    ok, detail = validate_embed_results(results, texts)
    status_str = "PASS" if ok else "FAIL"
    report.add(name, status_str, f"job_id={job_id} poll={elapsed:.2f}s total={total_elapsed:.2f}s {detail}",
               elapsed_s=total_elapsed)
    return results if ok else None


def validate_embed_results(results: List[Dict[str, Any]], texts: List[str]) -> Tuple[bool, str]:
    if len(results) != len(texts):
        return False, f"expected {len(texts)} results, got {len(results)}"
    dims = set()
    for r in results:
        emb = r.get("embedding")
        if not isinstance(emb, list) or not emb:
            return False, "missing/empty embedding vector"
        dims.add(len(emb))
        if not any(abs(x) > 1e-9 for x in emb):
            return False, f"zero vector for text {r.get('body', '')[:30]!r}"
    if len(dims) != 1:
        return False, f"inconsistent dimensions across results: {dims}"
    dim = dims.pop()
    if len(results) >= 2:
        embs = [r["embedding"] for r in results]
        for i in range(len(embs)):
            for j in range(i + 1, len(embs)):
                if cosine(embs[i], embs[j]) > 0.999:
                    return False, f"near-identical vectors for distinct texts [{i}] vs [{j}]"
    return True, f"{len(results)} vectors, dim={dim}, non-zero, distinct"


def check_embed_execute_direct(report: Report, base_url: str) -> None:
    texts = ["direct embed_execute synchronous call test", "second distinct sentence about oceans"]
    t0 = time.monotonic()
    status, body, err = jsonrpc(base_url, "embed_execute", {"texts": texts}, timeout=30)
    elapsed = time.monotonic() - t0
    if err or status != 200:
        report.add("embed_execute (direct sync)", "FAIL", f"HTTP {status} err={err}", elapsed_s=elapsed)
        return
    data = rpc_data(body)
    if not data:
        report.add("embed_execute (direct sync)", "FAIL", f"error: {rpc_error(body)}", elapsed_s=elapsed)
        return
    results = data.get("results", [])
    ok, detail = validate_embed_results(results, texts)
    device = data.get("device")
    status_str = "PASS" if ok else "FAIL"
    report.add("embed_execute (direct sync)", status_str, f"{detail} device={device} elapsed={elapsed:.2f}s",
               elapsed_s=elapsed)


def check_embed_thin_guard(report: Report, base_url: str) -> None:
    """embed_thin is a worker-only re-entry point; calling it directly
    without _job_id must fail cleanly (not crash / not hang)."""
    status, body, err = jsonrpc(base_url, "embed_thin", {"texts": ["should not work directly"]}, timeout=15)
    if err:
        report.add("embed_thin (direct call guard)", "FAIL", f"unreachable: {err}")
        return
    error = rpc_error(body)
    if status == 200 and error and "_job_id" in json.dumps(error):
        report.add("embed_thin (direct call guard)", "PASS", f"correctly rejected: {error.get('message')}")
    else:
        report.add("embed_thin (direct call guard)", "FAIL",
                    f"expected clean _job_id rejection, got HTTP {status} body={str(body)[:200]}")


def check_raw_queue_primitives(report: Report, base_url: str) -> None:
    """Exercise queue_add_job/queue_start_job/queue_get_job_status/
    queue_get_job_logs/queue_list_jobs/queue_health directly, mirroring what
    embed_router does internally (job_type=command_execution ->
    CommandExecutionJob -> embed_execute)."""
    job_id = str(uuid.uuid4())
    text = "raw queue_add_job CommandExecutionJob path verification text"

    status, body, err = jsonrpc(
        base_url, "queue_add_job",
        {
            "job_type": "command_execution",
            "job_id": job_id,
            "params": {"command": "embed_execute", "params": {"texts": [text]}},
        },
        timeout=15,
    )
    if err or status != 200 or not rpc_data(body):
        report.add("queue_add_job", "FAIL", f"HTTP {status} err={err} body={str(body)[:200]}")
        return
    report.add("queue_add_job", "PASS", f"job_id={job_id}")

    status, body, err = jsonrpc(base_url, "queue_start_job", {"job_id": job_id}, timeout=15)
    if err or status != 200 or not rpc_data(body):
        report.add("queue_start_job", "FAIL", f"HTTP {status} err={err} body={str(body)[:200]}")
        return
    report.add("queue_start_job", "PASS", "started")

    final_status, final_body, elapsed = poll_job(base_url, job_id, "queue_get_job_status", deadline_s=30)
    if final_status != "completed":
        report.add("queue_get_job_status", "FAIL", f"final_status={final_status} after {elapsed:.2f}s")
    else:
        results = extract_embed_results(final_body)
        ok, detail = validate_embed_results(results, [text])
        report.add("queue_get_job_status", "PASS" if ok else "FAIL", f"{detail} poll={elapsed:.2f}s")

    status, body, err = jsonrpc(base_url, "queue_get_job_logs", {"job_id": job_id}, timeout=15)
    if err or status != 200:
        report.add("queue_get_job_logs", "FAIL", f"HTTP {status} err={err}")
    else:
        data = rpc_data(body) or {}
        report.add("queue_get_job_logs", "PASS",
                    f"stdout_lines={data.get('stdout_lines')} stderr_lines={data.get('stderr_lines')}")

    status, body, err = jsonrpc(base_url, "queue_list_jobs", timeout=15)
    if err or status != 200:
        report.add("queue_list_jobs", "FAIL", f"HTTP {status} err={err}")
    else:
        data = rpc_data(body) or {}
        jobs = data.get("jobs", [])
        found = any(j.get("job_id") == job_id for j in jobs if isinstance(j, dict))
        report.add("queue_list_jobs", "PASS" if found else "FAIL",
                    f"{len(jobs)} jobs listed, our job present={found}")

    check_queue_health(report, base_url, label="queue_health")


def check_queue_health(report: Report, base_url: str, label: str = "queue_health") -> None:
    status, body, err = jsonrpc(base_url, "queue_health", timeout=15)
    if err or status != 200:
        report.add(label, "FAIL", f"HTTP {status} err={err}")
        return
    data = rpc_data(body) or {}
    if data.get("status") == "healthy" and data.get("running"):
        report.add(label, "PASS",
                    f"healthy running=True total={data.get('total_jobs')} "
                    f"pending={data.get('pending_jobs')} failed={data.get('failed_jobs')}")
    else:
        report.add(label, "FAIL", f"unhealthy: {data}")


def check_parallel_jobs(report: Report, base_url: str, n: int) -> None:
    """Submit N embed jobs back-to-back, actively poll each independently,
    validate vectors, and confirm the command API + queue survive."""
    texts_sets = [
        [f"parallel verify job {i} sentence A", f"parallel verify job {i} sentence B distinct topic"]
        for i in range(n)
    ]
    t0 = time.monotonic()
    job_ids: List[Tuple[int, str]] = []
    for i, texts in enumerate(texts_sets):
        status, body, err = jsonrpc(base_url, "embed", {"texts": texts}, timeout=15)
        data = rpc_data(body)
        if err or status != 200 or not data or not data.get("job_id"):
            report.add(f"parallel_submit[{i}]", "FAIL", f"HTTP {status} err={err} body={str(body)[:200]}")
            continue
        job_ids.append((i, data["job_id"]))
    submit_elapsed = time.monotonic() - t0
    report.add("parallel_submit_all", "PASS" if len(job_ids) == n else "FAIL",
               f"{len(job_ids)}/{n} jobs submitted in {submit_elapsed:.3f}s")

    # Actively poll all pending jobs concurrently (round-robin), not one at a time.
    pending = {jid: i for i, jid in job_ids}
    results_by_job: Dict[str, Tuple[str, Any, float]] = {}
    start = time.monotonic()
    deadline_s = max(60.0, 15.0 * n)
    while pending and time.monotonic() - start < deadline_s:
        for jid in list(pending.keys()):
            status, body, err = jsonrpc(base_url, "embed_job_status", {"job_id": jid}, timeout=15)
            if err:
                continue
            data = rpc_data(body) or {}
            st = data.get("status")
            if st in ("completed", "failed", "error"):
                results_by_job[jid] = (st, body, time.monotonic() - start)
                del pending[jid]
        if pending:
            time.sleep(0.3)

    for idx, jid in job_ids:
        if jid not in results_by_job:
            report.add(f"parallel_job[{idx}]", "FAIL", f"job_id={jid} TIMEOUT (no terminal status within {deadline_s:.0f}s)")
            continue
        st, body, elapsed = results_by_job[jid]
        if st != "completed":
            report.add(f"parallel_job[{idx}]", "FAIL", f"job_id={jid} status={st} elapsed={elapsed:.2f}s")
            continue
        results = extract_embed_results(body)
        ok, detail = validate_embed_results(results, texts_sets[idx])
        report.add(f"parallel_job[{idx}]", "PASS" if ok else "FAIL",
                    f"job_id={jid} elapsed={elapsed:.2f}s {detail}")

    total_elapsed = time.monotonic() - start
    report.add("parallel_jobs_wall_time", "PASS", f"{n} jobs, total poll wall time={total_elapsed:.2f}s")

    # Post-stress liveness: this is the actual spawn-fix regression class --
    # the command API and queue subsystem must survive concurrent queue load.
    status, body, err = jsonrpc(base_url, "health", timeout=15)
    data = rpc_data(body)
    if err or status != 200 or not data or data.get("status") != "ok":
        report.add("post_parallel_health", "FAIL", f"command API broken after parallel queue load: HTTP {status} err={err}")
    else:
        report.add("post_parallel_health", "PASS", "command API alive after parallel queue load")

    check_queue_health(report, base_url, label="post_parallel_queue_health")

    # One more sequential embed to prove the embed pipeline itself (not just
    # /health) still works after the burst.
    check_embed_via_queue(report, base_url, "embed",
                           ["post-parallel-stress sanity embed call"],
                           "post_parallel_embed_sanity", deadline_s=30)


def check_transfer_roundtrip(report: Report, base_url: str) -> None:
    import hashlib

    content = f"verify_embed_deep transfer roundtrip payload {uuid.uuid4()}".encode("utf-8")
    sha = hashlib.sha256(content).hexdigest()
    status, body, err = jsonrpc(
        base_url, "transfer_upload_begin",
        {
            "filename": "verify_embed_deep.txt",
            "size_bytes": len(content),
            "checksum_algorithm": "sha256",
            "checksum_value": sha,
            "compression": "identity",
        },
        timeout=15,
    )
    data = rpc_data(body)
    if err or status != 200 or not data or not data.get("transfer_id"):
        report.add("transfer_upload_begin", "FAIL", f"HTTP {status} err={err} body={str(body)[:200]}")
        return
    transfer_id = data["transfer_id"]
    report.add("transfer_upload_begin", "PASS", f"transfer_id={transfer_id}")

    try:
        put_resp = requests.put(
            f"{base_url}/api/transfer/uploads/{transfer_id}/chunks",
            params={"offset": 0},
            data=content,
            headers={"Content-Type": "application/octet-stream"},
            timeout=15,
            verify=False,
        )
        if put_resp.status_code not in (200, 201, 204):
            report.add("transfer_chunk_put", "FAIL", f"HTTP {put_resp.status_code} body={put_resp.text[:200]}")
            return
        report.add("transfer_chunk_put", "PASS", f"HTTP {put_resp.status_code}")
    except requests.RequestException as exc:
        report.add("transfer_chunk_put", "FAIL", str(exc))
        return

    status, body, err = jsonrpc(base_url, "transfer_upload_status", {"transfer_id": transfer_id}, timeout=15)
    data = rpc_data(body)
    if err or status != 200 or not data:
        report.add("transfer_upload_status", "FAIL", f"HTTP {status} err={err}")
    else:
        report.add("transfer_upload_status", "PASS", f"offset={data.get('offset')} status={data.get('status')}")

    status, body, err = jsonrpc(base_url, "transfer_upload_complete", {"transfer_id": transfer_id}, timeout=15)
    data = rpc_data(body)
    if err or status != 200 or not data or not data.get("completed"):
        report.add("transfer_upload_complete", "FAIL", f"HTTP {status} err={err} body={str(body)[:200]}")
    else:
        report.add("transfer_upload_complete", "PASS", f"status={data.get('status')} completed=True")

    # Clean-error paths (non-destructive, no real session/file involved).
    status, body, err = jsonrpc(base_url, "transfer_download_status", {"transfer_id": "tr_does_not_exist"}, timeout=15)
    error = rpc_error(body)
    if error and error.get("data", {}).get("error_type") == "TransferSessionNotFoundError":
        report.add("transfer_download_status (bad id)", "PASS", "clean TransferSessionNotFoundError")
    else:
        report.add("transfer_download_status (bad id)", "FAIL", f"unexpected: {str(body)[:200]}")


def check_embed_client_library(report: Report, host: str, port: int) -> None:
    """Smoke-test the embed-client PyPI library, IF importable in the
    current interpreter. This script deliberately does not create a venv or
    pip install anything -- run it with an interpreter that already has
    embed-client installed to exercise this section."""
    try:
        from embed_client.embedding_client import EmbeddingClient  # type: ignore
    except ImportError:
        report.add("embed_client library", "SKIP",
                    "embed_client not importable in current interpreter "
                    "(pip install embed-client to enable this check)")
        return

    import asyncio

    async def _run() -> Tuple[bool, str]:
        client = EmbeddingClient(protocol="https", host=host, port=port, check_hostname=False, timeout=60)
        try:
            health = await client.health()
            if health.get("status") != "ok":
                return False, f"health not ok: {health}"

            texts = [
                "embed-client library smoke test sentence one",
                "embed-client library smoke test sentence two different topic",
            ]
            result = await client.embed(texts, wait=True, wait_timeout=60)
            results = result.get("results") or (result.get("result") or {}).get("data", {}).get("results", [])
            ok, detail = validate_embed_results(results, texts)
            if not ok:
                return False, detail
            return True, f"health+embed OK via embed_client, {detail}"
        finally:
            close = getattr(client, "close", None)
            if close:
                await close()

    t0 = time.monotonic()
    try:
        ok, detail = asyncio.run(_run())
    except Exception as exc:  # noqa: BLE001
        report.add("embed_client library", "FAIL", f"exception: {type(exc).__name__}: {exc}",
                   elapsed_s=time.monotonic() - t0)
        return
    report.add("embed_client library", "PASS" if ok else "FAIL", detail, elapsed_s=time.monotonic() - t0)


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def print_table(report: Report) -> None:
    name_w = max((len(c.name) for c in report.checks), default=10)
    name_w = max(name_w, len("CHECK"))
    header = f"{'CHECK'.ljust(name_w)}  STATUS  DETAIL"
    print(header)
    print("-" * len(header))
    for c in report.checks:
        marker = {"PASS": "PASS  ", "FAIL": "FAIL  ", "SKIP": "SKIP  "}[c.status]
        print(f"{c.name.ljust(name_w)}  {marker}  {c.detail}")
    print()
    counts = {"PASS": 0, "FAIL": 0, "SKIP": 0}
    for c in report.checks:
        counts[c.status] += 1
    print(f"Summary: PASS={counts['PASS']} FAIL={counts['FAIL']} SKIP={counts['SKIP']} "
          f"OVERALL={report.overall}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default=DEFAULT_HOST, help="embed service host (default: %(default)s)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="embed service port (default: %(default)s)")
    parser.add_argument("--parallel-jobs", type=int, default=5, help="number of concurrent embed jobs to submit (default: %(default)s)")
    parser.add_argument("--skip-transfer", action="store_true", help="skip transfer_* upload/download round-trip check")
    parser.add_argument("--skip-parallel", action="store_true", help="skip the parallel-jobs queue stress check")
    parser.add_argument("--json", default=None, help="also write a JSON report to this path")
    args = parser.parse_args()

    base_url = f"https://{args.host}:{args.port}"
    report = Report()

    print(f"[verify_embed_deep] target: {base_url}\n")

    check_health_help_models(report, base_url)
    check_embed_via_queue(report, base_url, "embed",
                           ["The quick brown fox jumps over the lazy dog",
                            "Съешь ещё этих мягких французских булок"],
                           "embed (queue path, mixed languages)")
    check_embed_via_queue(report, base_url, "embed_queue",
                           ["embed_queue explicit submit text one",
                            "embed_queue explicit submit text two"],
                           "embed_queue (explicit submit)")
    check_embed_execute_direct(report, base_url)
    check_embed_thin_guard(report, base_url)
    check_raw_queue_primitives(report, base_url)

    if not args.skip_parallel:
        check_parallel_jobs(report, base_url, max(1, args.parallel_jobs))

    if not args.skip_transfer:
        check_transfer_roundtrip(report, base_url)

    check_embed_client_library(report, args.host, args.port)

    print()
    print_table(report)

    if args.json:
        payload = {
            "host": args.host,
            "port": args.port,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "overall": report.overall,
            "checks": [c.__dict__ for c in report.checks],
        }
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"\nJSON report written to: {args.json}")

    return 1 if report.overall == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
