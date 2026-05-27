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
    """
    global _last_written

    if not _enabled():
        return

    namespace = os.environ.get("NAMESPACE")
    configmap_name = os.environ.get("CONFIGMAP_NAME")
    if not namespace or not configmap_name:
        return

    payload = {k: str(v) for k, v in configmap.items()}

    with _lock:
        if _last_written == payload:
            return
        if _write(payload, namespace, configmap_name):
            _last_written = payload


def flush_status(**fields) -> bool:
    """Write a terminal status synchronously, bypassing dedup.

    Use this from signal handlers and just before process exit so that the
    final state (completed / failed / interrupted) is durable even if it is
    bit-equal to the previous write.
    """
    global _last_written

    if not _enabled():
        for k, v in fields.items():
            print(f"[status:{k}] {v}")
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
        return ok
