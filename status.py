"""Kubernetes ConfigMap status reporter for transfer progress.

Schema written to the ConfigMap (all values are strings; budcluster reads these
via `kubernetes.core.k8s_info` and treats them as a status payload):

    status            "downloading" | "completed" | "failed" | "interrupted"
    total_files       int
    completed_files   int
    total_size        int (bytes)
    completed_size    int (bytes)
    eta               int (seconds)
    reason            optional, present on failed / interrupted

Two entry points:

    update_status(data)        — used by the progress thread; deduplicates
                                  against the last successful write so an
                                  unchanged payload is a no-op.
    flush_status(**fields)     — synchronous, bypasses dedup; for terminal
                                  writes (completed / failed / interrupted)
                                  and signal handlers.
"""

import os
import threading
import time

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException


_lock = threading.Lock()
_last_written: dict | None = None
_k8s_client: client.CoreV1Api | None = None
_k8s_client_lock = threading.Lock()
# Once `flush_status` writes a terminal payload (completed / failed /
# interrupted), block any further `update_status` calls.  The progress
# thread may still be parked in a slow `replace_namespaced_config_map`
# call when the worker pool returns; without this guard, a stale
# "downloading" write can land AFTER the terminal status, and the
# budcluster polling workflow then never receives completion.
_terminated = threading.Event()


def _get_client() -> client.CoreV1Api:
    global _k8s_client
    with _k8s_client_lock:
        if _k8s_client is None:
            config.load_incluster_config()
            _k8s_client = client.CoreV1Api()
        return _k8s_client


def _write(
    data: dict, namespace: str, configmap_name: str, max_retries: int = 3
) -> bool:
    """Create-or-update the ConfigMap. Returns True on success."""
    v1 = _get_client()

    for attempt in range(max_retries):
        try:
            existing = v1.read_namespaced_config_map(configmap_name, namespace)
            if existing.data is None:
                existing.data = {}
            existing.data.update(data)
            v1.replace_namespaced_config_map(configmap_name, namespace, existing)
            return True

        except ApiException as e:
            if e.status == 404:
                try:
                    cm = client.V1ConfigMap(
                        metadata=client.V1ObjectMeta(name=configmap_name),
                        data=dict(data),
                    )
                    v1.create_namespaced_config_map(namespace=namespace, body=cm)
                    return True
                except ApiException as create_err:
                    if create_err.status == 409 and attempt < max_retries - 1:
                        time.sleep(0.1 * (2**attempt))
                        continue
                    print(f"[status] create failed: {create_err}")
                    return False
            elif e.status == 409 and attempt < max_retries - 1:
                time.sleep(0.1 * (2**attempt))
                continue
            else:
                print(f"[status] update failed: {e}")
                return False

    return False


def _enabled() -> bool:
    return os.environ.get("USE_KUBERNETES", "").lower() == "true"


def update_status(configmap: dict) -> None:
    """Write progress to the ConfigMap, skipping when nothing changed.

    Safe to call at high frequency — duplicates are dropped without an API call.
    Returns immediately once `flush_status` has emitted a terminal payload,
    so a slow in-flight progress write can't overwrite the terminal status.
    """
    global _last_written

    if not _enabled():
        return

    if _terminated.is_set():
        return

    namespace = os.environ.get("NAMESPACE")
    configmap_name = os.environ.get("CONFIGMAP_NAME")
    if not namespace or not configmap_name:
        return

    payload = {k: str(v) for k, v in configmap.items()}

    with _lock:
        if _terminated.is_set():
            # Re-check inside the lock: flush_status may have fired while
            # we were waiting.
            return
        if _last_written == payload:
            return
        if _write(payload, namespace, configmap_name):
            _last_written = payload


_TERMINAL_STATUSES = frozenset({"completed", "failed", "interrupted"})


def flush_status(**fields) -> bool:
    """Write a terminal status synchronously, bypassing dedup.

    Use this from signal handlers and just before process exit so that the
    final state (completed / failed / interrupted) is durable even if it is
    bit-equal to the previous write.  Once a terminal status has been
    written, subsequent ``update_status`` calls become no-ops so a slow
    progress-thread write can't clobber the terminal payload.
    """
    global _last_written

    payload_status = str(fields.get("status", "")).lower()
    if not _enabled():
        for k, v in fields.items():
            print(f"[status:{k}] {v}")
        if payload_status in _TERMINAL_STATUSES:
            _terminated.set()
        return True

    namespace = os.environ.get("NAMESPACE")
    configmap_name = os.environ.get("CONFIGMAP_NAME")
    if not namespace or not configmap_name:
        return False

    payload = {k: str(v) for k, v in fields.items()}
    with _lock:
        ok = _write(payload, namespace, configmap_name)
        if ok:
            _last_written = payload
        if payload_status in _TERMINAL_STATUSES:
            # Set even on a partial-success write; if the write failed we
            # don't want a steady-state progress thread to keep retrying
            # over the top of an in-progress flush either.
            _terminated.set()
        return ok
