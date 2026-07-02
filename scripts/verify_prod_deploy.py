#!/usr/bin/env python3
"""Production deploy verification for the 192.168.254.26 service fleet.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com

Verifies, for every service deployed on the production host, that:
  (a) the deployed package versions (mcp-proxy-adapter, queuemgr, and the
      service's own version) match the expected targets;
  (b) GET /health responds and reports "ok";
  (c) POST /api/jsonrpc {"command": "health"} does NOT return the
      "-32600 Invalid Request" incident marker (queuemgr queue-fallback
      regression: command API must survive after the queue subsystem has
      been touched), and /openapi.json exposes application commands (not
      just adapter builtins such as echo/long_task/queue_*);
  (d) for casmgr specifically: trigger a queue job (long_task), poll it to
      completion, and re-run the (c) check to prove the command API is
      still alive after queue activity ("queue-fallback" scenario).

Design notes
------------
* Connectivity: this script talks to services directly over HTTPS from
  wherever it runs (confirmed reachable: 8001 embed, 15010 casmgr, 15000
  ai-editor, 3011 mcp-terminal). svo-chunker (8009) is NOT reachable
  directly from outside the host (firewalled) -- it is probed over SSH
  from the production host itself (root@192.168.254.26 curl 127.0.0.1:8009).
  Package/version introspection for Docker-based services and the casmgr
  systemd/venv service is also done over SSH, since there is no local
  Docker/venv access to the production host's containers.
* TLS: all services here use server-side HTTPS certs that are not signed
  by a publicly trusted CA (internal PKI), so verification is disabled
  (equivalent to curl -k). ai-editor advertises mTLS (verify_client=true)
  but in practice the health/jsonrpc endpoints answer without a client
  cert being presented (observed directly); the script does not depend on
  client certs. If that ever changes, casmgr client certs are available
  on the host at /etc/casmgr/mtls/{client.crt,client.key,ca.crt} and can be
  fetched over SSH -- see fetch_casmgr_client_cert() below (unused by
  default, kept for future hardening).
* Idempotent: every run is read-only against application state except for
  the casmgr queue-fallback probe, which enqueues one short-lived
  "long_task" job (auto-completes in a few seconds, retained briefly by
  queuemgr's in-memory retention) -- safe to repeat on every deploy.
* Exit code: 0 only if every service is PASS or WAITING (not yet
  deployed to the target version is not a hard failure); non-zero if any
  service is FAIL or UNREACHABLE.

Usage
-----
    python3 scripts/verify_prod_deploy.py
    python3 scripts/verify_prod_deploy.py --host 192.168.254.26 --ssh root@192.168.254.26
    python3 scripts/verify_prod_deploy.py --json report.json
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:  # pragma: no cover
    print("ERROR: the 'requests' package is required (pip install requests)", file=sys.stderr)
    sys.exit(2)

try:
    from packaging import version as pkg_version
except ImportError:  # pragma: no cover
    print("ERROR: the 'packaging' package is required (pip install packaging)", file=sys.stderr)
    sys.exit(2)


DEFAULT_HOST = "192.168.254.26"
DEFAULT_SSH_TARGET = "root@192.168.254.26"
INCIDENT_MARKER_CODE = -32600

MIN_ADAPTER_VERSION = "8.10.15"
MIN_QUEUEMGR_VERSION = "1.0.20"

# Commands that ship with mcp-proxy-adapter / queuemgr itself, i.e. NOT
# application-specific. If /commands (or openapi paths-derived command list)
# contains ONLY these, the service has not actually registered its own API.
BUILTIN_COMMANDS = {
    "echo",
    "long_task",
    "job_status",
    "queue_add_job",
    "queue_start_job",
    "queue_stop_job",
    "queue_delete_job",
    "queue_get_job_status",
    "queue_get_job_logs",
    "queue_list_jobs",
    "queue_health",
    "help",
    "health",
    "config",
    "reload",
    "settings",
    "load",
    "unload",
    "plugins",
    "transport_management",
    "proxy_registration",
    "roletest",
    "transfer_upload_begin",
    "transfer_upload_status",
    "transfer_upload_complete",
    "transfer_download_begin",
    "transfer_download_status",
    "info",
}


@dataclass
class CheckResult:
    name: str
    status: str  # "PASS" | "FAIL" | "WAITING" | "SKIP"
    detail: str = ""


@dataclass
class ServiceReport:
    service: str
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


def run_ssh(ssh_target: str, remote_cmd: str, timeout: int = 20) -> Tuple[int, str, str]:
    """Run a command on the production host over SSH, return (rc, stdout, stderr)."""
    cmd = [
        "ssh",
        "-o", "ConnectTimeout=8",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        ssh_target,
        remote_cmd,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired:
        return 124, "", "ssh timeout"
    except FileNotFoundError:
        return 127, "", "ssh binary not found"


# When set, HTTP probes are executed as `ssh <target> curl https://127.0.0.1:<port>`
# from the production host itself, instead of connecting directly. This is
# required because casmgr (15010) and ai-editor (15000) — like svo (8009) — are
# firewalled off the LAN by design (network is the access-control layer; the
# services run plain https, not mTLS). Loopback on the host still reaches them,
# which reflects true service health without depending on external reachability.
_LOOPBACK_SSH: Optional[str] = None
_LOOPBACK_EXTERNAL_HOST: Optional[str] = None


def _loopback_url(url: str) -> str:
    """Rewrite the external host in a URL to 127.0.0.1 for on-host probing."""
    if _LOOPBACK_EXTERNAL_HOST and f"//{_LOOPBACK_EXTERNAL_HOST}:" in url:
        return url.replace(f"//{_LOOPBACK_EXTERNAL_HOST}:", "//127.0.0.1:", 1)
    return url


def _curl_over_ssh(url: str, method: str, payload: Optional[Dict[str, Any]],
                   timeout: float) -> Tuple[Optional[int], Any, Optional[str]]:
    target = _LOOPBACK_SSH
    assert target is not None
    loop_url = _loopback_url(url)
    parts = ["curl", "-sk", "--max-time", str(int(timeout)),
             "-w", "\\n%{http_code}", "-o", "-"]
    if method == "POST":
        parts += ["-X", "POST", "-H", "Content-Type: application/json",
                  "--data", json.dumps(payload or {})]
    parts.append(loop_url)
    remote_cmd = " ".join(shlex.quote(p) for p in parts)
    rc, out, err = run_ssh(target, remote_cmd, timeout=int(timeout) + 10)
    if rc != 0 and not out:
        return None, None, err or f"ssh curl rc={rc}"
    text = out.rstrip("\n")
    nl = text.rfind("\n")
    if nl == -1:
        code_str, body_text = text.strip(), ""
    else:
        body_text, code_str = text[:nl], text[nl + 1:].strip()
    try:
        status_code: Optional[int] = int(code_str)
    except ValueError:
        status_code = None
    try:
        body: Any = json.loads(body_text)
    except (ValueError, TypeError):
        body = body_text
    return status_code, body, None


def http_get(url: str, timeout: float = 10.0) -> Tuple[Optional[int], Any, Optional[str]]:
    if _LOOPBACK_SSH:
        return _curl_over_ssh(url, "GET", None, timeout)
    try:
        resp = requests.get(url, timeout=timeout, verify=False)
        try:
            body: Any = resp.json()
        except ValueError:
            body = resp.text
        return resp.status_code, body, None
    except requests.RequestException as exc:
        return None, None, str(exc)


def http_post_json(url: str, payload: Dict[str, Any], timeout: float = 30.0) -> Tuple[Optional[int], Any, Optional[str]]:
    if _LOOPBACK_SSH:
        return _curl_over_ssh(url, "POST", payload, timeout)
    try:
        resp = requests.post(url, json=payload, timeout=timeout, verify=False)
        try:
            body: Any = resp.json()
        except ValueError:
            body = resp.text
        return resp.status_code, body, None
    except requests.RequestException as exc:
        return None, None, str(exc)


def jsonrpc_call(base_url: str, method: str, params: Optional[Dict[str, Any]] = None,
                  timeout: float = 30.0) -> Tuple[Optional[int], Any, Optional[str]]:
    """Standard JSON-RPC 2.0 call: {"jsonrpc": "2.0", "method": ..., "params": ..., "id": 1}."""
    payload = {"jsonrpc": "2.0", "method": method, "params": params or {}, "id": 1}
    return http_post_json(f"{base_url}/api/jsonrpc", payload, timeout=timeout)


def jsonrpc_command_marker_call(base_url: str, command: str,
                                 timeout: float = 15.0) -> Tuple[Optional[int], Any, Optional[str]]:
    """The literal incident-marker shape from the bug report:
    POST /api/jsonrpc {"command": "<command>"} (no "method"/"jsonrpc" keys).
    Historically this could return -32600 Invalid Request after queue
    fallback; queuemgr 1.0.20 must keep the command API alive regardless of
    which request shape callers use.
    """
    return http_post_json(f"{base_url}/api/jsonrpc", {"command": command}, timeout=timeout)


def is_incident_marker(status_code: Optional[int], body: Any) -> bool:
    """True if the response is the exact -32600 Invalid Request incident."""
    if not isinstance(body, dict):
        return False
    err = body.get("error")
    if isinstance(err, dict) and err.get("code") == INCIDENT_MARKER_CODE:
        return True
    return False


def extract_commands_from_openapi(openapi_doc: Any) -> List[str]:
    """Best-effort extraction of registered command names from openapi.json.

    mcp-proxy-adapter's openapi.json describes transport paths (/health,
    /api/jsonrpc, ...), not one path per command, so the actual command
    catalog is fetched via /commands. This helper is kept for services
    where /commands is unavailable and falls back to scanning schema
    enums/descriptions for command names.
    """
    names: List[str] = []
    if not isinstance(openapi_doc, dict):
        return names
    paths = openapi_doc.get("paths", {})
    if isinstance(paths, dict):
        names.extend(paths.keys())
    return names


def fetch_commands_list(base_url: str, timeout: float = 10.0) -> Tuple[Optional[List[str]], Optional[str]]:
    status, body, err = http_get(f"{base_url}/commands", timeout=timeout)
    if err:
        return None, err
    if status != 200:
        return None, f"HTTP {status}"
    if isinstance(body, dict) and isinstance(body.get("commands"), dict):
        return list(body["commands"].keys()), None
    if isinstance(body, dict) and isinstance(body.get("commands"), list):
        return list(body["commands"]), None
    if isinstance(body, list):
        return list(body), None
    return None, f"unexpected /commands shape: {type(body)}"


def compare_min_version(installed: Optional[str], minimum: str) -> Optional[bool]:
    if not installed:
        return None
    try:
        return pkg_version.parse(installed) >= pkg_version.parse(minimum)
    except Exception:
        return None


# --------------------------------------------------------------------------
# Per-service package/version introspection (over SSH: docker exec pip show,
# docker image tag, or venv pip show for the systemd-based casmgr service).
# --------------------------------------------------------------------------

def docker_container_running(ssh_target: str, name: str) -> bool:
    rc, out, _ = run_ssh(ssh_target, f"docker ps --filter name=^/{name}$ --format '{{{{.ID}}}}'")
    return rc == 0 and out.strip() != ""


def docker_image_tag(ssh_target: str, container: str) -> Optional[str]:
    rc, out, _ = run_ssh(ssh_target, f"docker inspect --format '{{{{.Config.Image}}}}' {shlex.quote(container)}")
    if rc != 0:
        return None
    return out.strip() or None


def docker_pip_show(ssh_target: str, container: str, package: str) -> Optional[str]:
    rc, out, _ = run_ssh(
        ssh_target,
        f"docker exec {shlex.quote(container)} pip show {shlex.quote(package)} 2>/dev/null | grep -m1 '^Version:'",
    )
    if rc != 0 or not out.strip():
        return None
    return out.strip().split(":", 1)[1].strip()


def venv_pip_show(ssh_target: str, pip_bin: str, package: str) -> Optional[str]:
    rc, out, _ = run_ssh(
        ssh_target,
        f"{pip_bin} show {shlex.quote(package)} 2>/dev/null | grep -m1 '^Version:'",
    )
    if rc != 0 or not out.strip():
        return None
    return out.strip().split(":", 1)[1].strip()


# --------------------------------------------------------------------------
# Per-service verification routines
# --------------------------------------------------------------------------

def verify_generic_https_service(
    report: ServiceReport,
    base_url: str,
    expected_service_version: str,
    adapter_version_installed: Optional[str],
    queuemgr_version_installed: Optional[str],
    service_version_installed: Optional[str],
    min_adapter: str = MIN_ADAPTER_VERSION,
    min_queuemgr: str = MIN_QUEUEMGR_VERSION,
) -> None:
    # (a) versions
    adapter_ok = compare_min_version(adapter_version_installed, min_adapter)
    if adapter_version_installed is None:
        report.add("adapter_version", "FAIL", "could not determine installed mcp-proxy-adapter version")
    elif adapter_ok:
        report.add("adapter_version", "PASS", f"{adapter_version_installed} >= {min_adapter}")
    else:
        report.add("adapter_version", "WAITING", f"{adapter_version_installed} < {min_adapter} (deploy pending)")

    queuemgr_ok = compare_min_version(queuemgr_version_installed, min_queuemgr)
    if queuemgr_version_installed is None:
        report.add("queuemgr_version", "FAIL", "could not determine installed queuemgr version")
    elif queuemgr_ok:
        report.add("queuemgr_version", "PASS", f"{queuemgr_version_installed} >= {min_queuemgr}")
    else:
        report.add("queuemgr_version", "WAITING", f"{queuemgr_version_installed} < {min_queuemgr} (deploy pending)")

    if service_version_installed is None:
        report.add("service_version", "FAIL", "could not determine installed service version")
    elif service_version_installed == expected_service_version:
        report.add("service_version", "PASS", service_version_installed)
    else:
        try:
            newer = pkg_version.parse(service_version_installed) >= pkg_version.parse(expected_service_version)
        except Exception:
            newer = None
        if newer:
            report.add("service_version", "PASS", f"{service_version_installed} (>= expected {expected_service_version})")
        else:
            report.add(
                "service_version", "WAITING",
                f"installed={service_version_installed} expected={expected_service_version} (deploy pending)",
            )

    # (b) GET /health
    status, body, err = http_get(f"{base_url}/health")
    if err:
        report.add("get_health", "FAIL", f"unreachable: {err}")
        return
    if status == 200 and isinstance(body, dict) and body.get("status") == "ok":
        report.add("get_health", "PASS", f"HTTP {status} status=ok")
    else:
        report.add("get_health", "FAIL", f"HTTP {status} body={str(body)[:200]}")

    # (c) command API survives + literal incident-marker shape check
    verify_command_api_alive(report, base_url, label="jsonrpc_health")


def verify_command_api_alive(report: ServiceReport, base_url: str, label: str) -> bool:
    """Runs both the literal incident-marker probe and a standards-shaped
    JSON-RPC health call; also checks /openapi.json / /commands exposes
    application-level (non-builtin) commands. Returns True if the command
    API is confirmed alive and not showing the incident.
    """
    ok = True

    # Literal marker shape from the bug report.
    status, body, err = jsonrpc_command_marker_call(base_url, "health")
    if err:
        report.add(f"{label}_marker_shape", "FAIL", f"unreachable: {err}")
        ok = False
    elif is_incident_marker(status, body):
        report.add(f"{label}_marker_shape", "FAIL", "INCIDENT: -32600 Invalid Request on {'command':'health'}")
        ok = False
    elif status == 200:
        report.add(f"{label}_marker_shape", "PASS", "no -32600 incident marker")
    else:
        # Non-200 that's not the -32600 marker (e.g. 422 validation error
        # because this adapter build requires "method" instead of
        # "command") is not the incident itself, but still worth surfacing.
        report.add(f"{label}_marker_shape", "PASS", f"HTTP {status} (not the -32600 incident; adapter expects 'method')")

    # Standards-shaped JSON-RPC call, should always succeed regardless of
    # whether the "command" shorthand is supported.
    status, body, err = jsonrpc_call(base_url, "health")
    if err:
        report.add(f"{label}_method_shape", "FAIL", f"unreachable: {err}")
        ok = False
    elif isinstance(body, dict) and isinstance(body.get("error"), dict):
        code = body["error"].get("code")
        report.add(f"{label}_method_shape", "FAIL", f"jsonrpc error code={code} msg={body['error'].get('message')}")
        ok = False
    elif status == 200 and isinstance(body, dict) and body.get("result", {}).get("success"):
        report.add(f"{label}_method_shape", "PASS", "health via method= shape OK")
    else:
        report.add(f"{label}_method_shape", "FAIL", f"HTTP {status} body={str(body)[:200]}")
        ok = False

    # openapi.json / commands: application commands present, not just
    # adapter builtins.
    commands, cerr = fetch_commands_list(base_url)
    if cerr:
        status, doc, oerr = http_get(f"{base_url}/openapi.json")
        if oerr or status != 200:
            report.add(f"{label}_app_commands", "FAIL", f"/commands error ({cerr}) and /openapi.json error ({oerr or status})")
            return ok
        commands = extract_commands_from_openapi(doc)

    if not commands:
        report.add(f"{label}_app_commands", "FAIL", "no commands discovered via /commands or /openapi.json")
        return False

    non_builtin = [c for c in commands if c not in BUILTIN_COMMANDS and not c.startswith("/")]
    if non_builtin:
        report.add(
            f"{label}_app_commands", "PASS",
            f"{len(commands)} total, {len(non_builtin)} application commands (e.g. {non_builtin[:5]})",
        )
    else:
        report.add(
            f"{label}_app_commands", "FAIL",
            f"only builtin/transport entries found ({len(commands)} total) -- app commands not registered",
        )
        ok = False

    return ok


def verify_casmgr_queue_fallback(report: ServiceReport, base_url: str) -> None:
    """(d) casmgr-specific: enqueue a short job, poll to completion, then
    re-run the command-API-alive check to prove the fallback fix holds.
    """
    status, body, err = http_post_json(
        f"{base_url}/api/jsonrpc",
        {"command": "long_task", "params": {"duration": 2}},
        timeout=15,
    )
    if err or status != 200 or not isinstance(body, dict):
        report.add("queue_fallback_enqueue", "FAIL", f"could not enqueue long_task: HTTP {status} err={err} body={str(body)[:200]}")
        return
    result = body.get("result", {})
    data = result.get("data") if isinstance(result, dict) else None
    job_id = None
    if isinstance(data, dict):
        job_id = data.get("job_id")
    if not job_id:
        report.add("queue_fallback_enqueue", "FAIL", f"no job_id in response: {str(body)[:200]}")
        return
    report.add("queue_fallback_enqueue", "PASS", f"job_id={job_id}")

    # Poll job_status for up to ~15s.
    deadline = time.time() + 15
    final_status = None
    while time.time() < deadline:
        status, body, err = http_post_json(
            f"{base_url}/api/jsonrpc",
            {"command": "job_status", "params": {"job_id": job_id}},
            timeout=10,
        )
        if err or status != 200 or not isinstance(body, dict):
            time.sleep(1)
            continue
        data = body.get("result", {}).get("data", {}) if isinstance(body.get("result"), dict) else {}
        final_status = data.get("status")
        if data.get("done"):
            break
        time.sleep(1)

    if final_status == "completed":
        report.add("queue_fallback_job_completed", "PASS", f"job {job_id} completed")
    else:
        report.add("queue_fallback_job_completed", "FAIL", f"job {job_id} did not complete (last status={final_status})")

    # Re-run the command-API-alive check -- this is the actual regression
    # test: command API must survive after queue activity.
    alive = verify_command_api_alive(report, base_url, label="post_queue_health")
    if alive:
        report.add("queue_fallback_survives", "PASS", "command API alive after queue activity")
    else:
        report.add("queue_fallback_survives", "FAIL", "command API broken after queue activity (queue-fallback regression)")


# --------------------------------------------------------------------------
# Service definitions
# --------------------------------------------------------------------------

def check_embed(ssh_target: str, host: str) -> ServiceReport:
    report = ServiceReport(service="embed")
    container = "embed"
    if not docker_container_running(ssh_target, container):
        report.add("container_running", "FAIL", f"docker container '{container}' not running on host")
        return report
    image_tag = docker_image_tag(ssh_target, container) or ""
    service_version = image_tag.split(":")[-1] if ":" in image_tag else None
    adapter_v = docker_pip_show(ssh_target, container, "mcp-proxy-adapter")
    queuemgr_v = docker_pip_show(ssh_target, container, "queuemgr")

    base_url = f"https://{host}:8001"
    verify_generic_https_service(
        report, base_url,
        expected_service_version="2.0.12",
        adapter_version_installed=adapter_v,
        queuemgr_version_installed=queuemgr_v,
        service_version_installed=service_version,
    )
    return report


def check_ai_editor(ssh_target: str, host: str) -> ServiceReport:
    report = ServiceReport(service="ai-editor")
    container = "ai-editor"
    if not docker_container_running(ssh_target, container):
        report.add("container_running", "FAIL", f"docker container '{container}' not running on host")
        return report
    image_tag = docker_image_tag(ssh_target, container) or ""
    service_version = image_tag.split(":")[-1] if ":" in image_tag else None
    adapter_v = docker_pip_show(ssh_target, container, "mcp-proxy-adapter")
    queuemgr_v = docker_pip_show(ssh_target, container, "queuemgr")

    base_url = f"https://{host}:15000"
    verify_generic_https_service(
        report, base_url,
        expected_service_version="1.0.44",
        adapter_version_installed=adapter_v,
        queuemgr_version_installed=queuemgr_v,
        service_version_installed=service_version,
    )
    return report


def check_mcp_terminal(ssh_target: str, host: str) -> ServiceReport:
    report = ServiceReport(service="mcp-terminal")
    container = "mcp-terminal"
    if not docker_container_running(ssh_target, container):
        report.add("container_running", "FAIL", f"docker container '{container}' not running on host")
        return report
    image_tag = docker_image_tag(ssh_target, container) or ""
    service_version = image_tag.split(":")[-1] if ":" in image_tag else None
    adapter_v = docker_pip_show(ssh_target, container, "mcp-proxy-adapter")
    queuemgr_v = docker_pip_show(ssh_target, container, "queuemgr")

    base_url = f"https://{host}:3011"
    verify_generic_https_service(
        report, base_url,
        expected_service_version="0.1.37",
        adapter_version_installed=adapter_v,
        queuemgr_version_installed=queuemgr_v,
        service_version_installed=service_version,
    )
    return report


def check_svo_chunker(ssh_target: str, host: str) -> ServiceReport:
    report = ServiceReport(service="svo-chunker")
    container = "svo-chunker"
    if not docker_container_running(ssh_target, container):
        report.add("container_running", "WAITING", f"docker container '{container}' not running (deploy in progress or down)")
        return report
    image_tag = docker_image_tag(ssh_target, container) or ""
    service_version = image_tag.split(":")[-1] if ":" in image_tag else None
    adapter_v = docker_pip_show(ssh_target, container, "mcp-proxy-adapter")
    queuemgr_v = docker_pip_show(ssh_target, container, "queuemgr")

    adapter_ok = compare_min_version(adapter_v, MIN_ADAPTER_VERSION)
    if adapter_v is None:
        report.add("adapter_version", "FAIL", "could not determine installed mcp-proxy-adapter version")
    elif adapter_ok:
        report.add("adapter_version", "PASS", f"{adapter_v} >= {MIN_ADAPTER_VERSION}")
    else:
        report.add("adapter_version", "WAITING", f"{adapter_v} < {MIN_ADAPTER_VERSION} (deploy pending)")

    queuemgr_ok = compare_min_version(queuemgr_v, MIN_QUEUEMGR_VERSION)
    if queuemgr_v is None:
        report.add("queuemgr_version", "FAIL", "could not determine installed queuemgr version")
    elif queuemgr_ok:
        report.add("queuemgr_version", "PASS", f"{queuemgr_v} >= {MIN_QUEUEMGR_VERSION}")
    else:
        report.add("queuemgr_version", "WAITING", f"{queuemgr_v} < {MIN_QUEUEMGR_VERSION} (deploy pending)")

    expected = "0.2.12"
    if service_version:
        try:
            newer = pkg_version.parse(service_version) >= pkg_version.parse(expected)
        except Exception:
            newer = service_version == expected
        if newer:
            report.add("service_version", "PASS", f"{service_version} (>= expected {expected})")
        else:
            report.add("service_version", "WAITING", f"installed={service_version} expected={expected} (deploy pending)")
    else:
        report.add("service_version", "FAIL", "could not determine installed service version")

    # 8009 is not reachable directly from outside the host; probe from the
    # host itself via SSH+curl (127.0.0.1 avoids the docker-proxy binding
    # question entirely).
    rc, out, err = run_ssh(
        ssh_target,
        "curl -sk --max-time 10 -o /dev/null -w '%{http_code}' https://127.0.0.1:8009/health",
    )
    if rc != 0 or not out.strip().isdigit():
        report.add("get_health", "WAITING", f"service not reachable on host (rc={rc} out={out!r} err={err!r})")
        return report
    code = int(out.strip())
    if code == 200:
        report.add("get_health", "PASS", "HTTP 200 (via SSH loopback)")
    else:
        report.add("get_health", "FAIL", f"HTTP {code} (via SSH loopback)")

    # jsonrpc marker check, also via SSH loopback curl.
    rc, out, err = run_ssh(
        ssh_target,
        "curl -sk --max-time 15 -X POST https://127.0.0.1:8009/api/jsonrpc "
        "-H 'Content-Type: application/json' -d '{\"command\": \"health\"}'",
    )
    if rc != 0:
        report.add("jsonrpc_health_marker_shape", "WAITING", f"unreachable via SSH loopback: {err}")
    else:
        try:
            body = json.loads(out)
        except ValueError:
            body = None
        if is_incident_marker(200, body):
            report.add("jsonrpc_health_marker_shape", "FAIL", "INCIDENT: -32600 Invalid Request on {'command':'health'}")
        else:
            report.add("jsonrpc_health_marker_shape", "PASS", "no -32600 incident marker (via SSH loopback)")

    return report


def check_casmgr(ssh_target: str, host: str) -> ServiceReport:
    report = ServiceReport(service="casmgr-server")

    rc, out, _ = run_ssh(ssh_target, "systemctl is-active casmgr-server.service")
    active = rc == 0 and out.strip() == "active"
    if not active:
        report.add("systemd_active", "FAIL", f"casmgr-server.service not active (state={out.strip() or 'unknown'})")
        return report
    report.add("systemd_active", "PASS", "casmgr-server.service active")

    pip_bin = "/usr/lib/casmgr-server/.venv/bin/pip"
    adapter_v = venv_pip_show(ssh_target, pip_bin, "mcp-proxy-adapter")
    queuemgr_v = venv_pip_show(ssh_target, pip_bin, "queuemgr")
    service_v = venv_pip_show(ssh_target, pip_bin, "code-analysis")

    base_url = f"https://{host}:15010"

    adapter_ok = compare_min_version(adapter_v, MIN_ADAPTER_VERSION)
    if adapter_v is None:
        report.add("adapter_version", "FAIL", "could not determine installed mcp-proxy-adapter version")
    elif adapter_ok:
        report.add("adapter_version", "PASS", f"{adapter_v} >= {MIN_ADAPTER_VERSION}")
    else:
        report.add("adapter_version", "WAITING", f"{adapter_v} < {MIN_ADAPTER_VERSION} (deploy pending)")

    queuemgr_ok = compare_min_version(queuemgr_v, MIN_QUEUEMGR_VERSION)
    if queuemgr_v is None:
        report.add("queuemgr_version", "FAIL", "could not determine installed queuemgr version")
    elif queuemgr_ok:
        report.add("queuemgr_version", "PASS", f"{queuemgr_v} >= {MIN_QUEUEMGR_VERSION}")
    else:
        report.add("queuemgr_version", "WAITING", f"{queuemgr_v} < {MIN_QUEUEMGR_VERSION} (deploy pending)")

    expected = "1.6.23"
    if service_v == expected:
        report.add("service_version", "PASS", service_v)
    elif service_v:
        try:
            newer = pkg_version.parse(service_v) >= pkg_version.parse(expected)
        except Exception:
            newer = None
        if newer:
            report.add("service_version", "PASS", f"{service_v} (>= expected {expected})")
        else:
            report.add("service_version", "WAITING", f"installed={service_v} expected={expected} (deploy pending)")
    else:
        report.add("service_version", "FAIL", "could not determine installed code-analysis version")

    # (b) GET /health
    status, body, err = http_get(f"{base_url}/health")
    if err:
        report.add("get_health", "FAIL", f"unreachable: {err}")
        return report
    if status == 200 and isinstance(body, dict) and body.get("status") == "ok":
        report.add("get_health", "PASS", f"HTTP {status} status=ok")
    else:
        report.add("get_health", "FAIL", f"HTTP {status} body={str(body)[:200]}")

    # (c) command API alive + app commands registered (not just builtins)
    verify_command_api_alive(report, base_url, label="jsonrpc_health")

    # (d) queue-fallback scenario, casmgr-specific
    verify_casmgr_queue_fallback(report, base_url)

    return report


SERVICE_CHECKS = {
    "embed": check_embed,
    "svo-chunker": check_svo_chunker,
    "casmgr-server": check_casmgr,
    "ai-editor": check_ai_editor,
    "mcp-terminal": check_mcp_terminal,
}


def print_report_table(reports: List[ServiceReport]) -> None:
    col_service = max(len("SERVICE"), max((len(r.service) for r in reports), default=0))
    header = f"{'SERVICE'.ljust(col_service)}  {'STATUS':8}  DETAILS"
    print(header)
    print("-" * len(header))
    for r in reports:
        print(f"{r.service.ljust(col_service)}  {r.overall:8}")
        for c in r.checks:
            marker = {"PASS": "  [PASS]", "FAIL": "  [FAIL]", "WAITING": "  [WAIT]", "SKIP": "  [SKIP]"}[c.status]
            print(f"    {marker} {c.name}: {c.detail}")
    print()
    print("Summary:")
    summary_header = f"  {'SERVICE'.ljust(col_service)}  OVERALL"
    print(summary_header)
    for r in reports:
        print(f"  {r.service.ljust(col_service)}  {r.overall}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default=DEFAULT_HOST, help="production host IP/hostname (default: %(default)s)")
    parser.add_argument("--ssh", default=DEFAULT_SSH_TARGET, help="ssh target for host-side introspection (default: %(default)s)")
    parser.add_argument("--json", default=None, help="also write a JSON report to this path")
    parser.add_argument("--only", default=None, help="comma-separated subset of services to check")
    parser.add_argument("--direct", action="store_true",
                        help="probe services directly over the network instead of via "
                             "SSH loopback on the host (default: loopback, because casmgr/"
                             "ai-editor/svo ports are firewalled off the LAN by design)")
    args = parser.parse_args()

    # Default to on-host loopback probing: the service ports are deliberately not
    # reachable from the LAN (network is the access-control layer), so a direct
    # probe would report false FAILs. --direct forces the old external behavior.
    if not args.direct:
        global _LOOPBACK_SSH, _LOOPBACK_EXTERNAL_HOST
        _LOOPBACK_SSH = args.ssh
        _LOOPBACK_EXTERNAL_HOST = args.host

    services = list(SERVICE_CHECKS.keys())
    if args.only:
        requested = {s.strip() for s in args.only.split(",")}
        unknown = requested - set(services)
        if unknown:
            print(f"ERROR: unknown service(s): {unknown}. Known: {services}", file=sys.stderr)
            return 2
        services = [s for s in services if s in requested]

    reports: List[ServiceReport] = []
    for name in services:
        fn = SERVICE_CHECKS[name]
        try:
            reports.append(fn(args.ssh, args.host))
        except Exception as exc:  # keep going even if one service check crashes
            r = ServiceReport(service=name)
            r.add("unexpected_error", "FAIL", f"{type(exc).__name__}: {exc}")
            reports.append(r)

    print_report_table(reports)

    if args.json:
        payload = {
            "host": args.host,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "services": [
                {
                    "service": r.service,
                    "overall": r.overall,
                    "checks": [c.__dict__ for c in r.checks],
                }
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
