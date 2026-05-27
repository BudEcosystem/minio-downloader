"""Resumable, parallel object transfer between MinIO/S3 and a local PVC.

Design contract with budcluster (see services/budcluster/budcluster/cluster_ops/
kubernetes.py::get_model_transfer_status):

  * Progress is reported through a ConfigMap (`status.py`) using the existing
    schema (status / total_files / completed_files / total_size /
    completed_size / eta).
  * `status` values: "downloading", "completed", "failed", or "interrupted".
    The new "interrupted" value lets the workflow distinguish a SIGTERM'd
    pod (resume on next attempt, do not fail the workflow) from a real
    "failed" terminal state.

Resumability
------------

Downloads stream into `<final_path>.part` and rename atomically on
completion.  On startup of each file:

  * size match + etag-sidecar match  → skip (idempotent re-run)
  * size match, no sidecar           → trust + write sidecar (legacy files)
  * size mismatch                    → discard and re-download
  * `.part` size  ≤ server size      → resume with Range: bytes=offset-
  * `.part` size  >  server size     → discard and re-download

For files at or above `MULTIPART_THRESHOLD_BYTES` (default 256 MiB), the
download is split across `INTRAFILE_WORKERS` parallel ranged GETs writing
into a pre-allocated `.part` file.  Completed chunk offsets are persisted in
a `.part.idx` JSON file so a fresh pod can re-derive what's left.

Everything is driven by a shared `threading.Event` (`shutdown`) so SIGTERM
unwinds promptly with a partial state on disk that the next invocation can
pick up from.
"""

import json
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Iterable, Optional

from minio import Minio
from minio.error import S3Error
from urllib3.exceptions import HTTPError, ProtocolError

from status import flush_status, update_status


# ---------------------------------------------------------------------------
# Tunables (env-overridable)
# ---------------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default


OUTER_WORKERS = _env_int("OUTER_WORKERS", 8)
INTRAFILE_WORKERS = _env_int("INTRAFILE_WORKERS", 8)
MULTIPART_THRESHOLD_BYTES = _env_int("MULTIPART_THRESHOLD_BYTES", 256 * 1024 * 1024)
MULTIPART_PART_SIZE_BYTES = _env_int("MULTIPART_PART_SIZE_BYTES", 32 * 1024 * 1024)
STREAM_CHUNK_BYTES = _env_int("STREAM_CHUNK_BYTES", 1024 * 1024)
NETWORK_RETRY_ATTEMPTS = _env_int("NETWORK_RETRY_ATTEMPTS", 5)
STATUS_UPDATE_INTERVAL_SECS = _env_int("STATUS_UPDATE_INTERVAL_SECS", 2)


# Errors that should be retried transparently inside a single download.  S3
# 5xx / connection reset / partial-read all fall into here; auth / 404 do not.
_RETRYABLE_EXC = (ProtocolError, HTTPError, ConnectionError, TimeoutError)


# ---------------------------------------------------------------------------
# Progress tracking (kept compatible with existing tests)
# ---------------------------------------------------------------------------


class ProgressCallback(threading.Thread):
    """Adapter so minio-py's progress hook can drive `TransferProgress`."""

    def __init__(self, transfer_progress: "TransferProgress", file_id: str):
        threading.Thread.__init__(self)
        self.daemon = True
        self.transfer_progress = transfer_progress
        self.file_id = file_id
        self.total_length = 0
        self.current_size = 0

    def set_meta(self, total_length, object_name):
        self.total_length = total_length
        self.object_name = object_name
        self.transfer_progress.start_file(self.file_id, total_length)

    def update(self, size):
        self.current_size += size
        self.transfer_progress.update_file(self.file_id, self.current_size)

    def run(self):
        pass


class TransferProgress:
    """Aggregate progress + speed + ETA across the whole transfer."""

    def __init__(self, total_files: int, total_size: int):
        self.total_files = total_files
        self.total_size = total_size
        self.completed_files = 0
        self.completed_size = 0
        self.skipped_files = 0
        self.skipped_size = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
        self.file_progress: dict = {}
        self.speed_samples: deque = deque(maxlen=10)
        self.last_update_time = self.start_time

    def start_file(self, file_id, file_size):
        with self.lock:
            self.file_progress[file_id] = {
                "size": file_size,
                "transferred": 0,
                "start_time": time.time(),
            }

    def update_file(self, file_id, bytes_transferred):
        with self.lock:
            entry = self.file_progress.get(file_id)
            if entry is None:
                return
            old_transferred = entry["transferred"]
            entry["transferred"] = bytes_transferred
            now = time.time()
            dt = now - self.last_update_time
            if dt > 0:
                delta = bytes_transferred - old_transferred
                if delta > 0:
                    self.speed_samples.append(delta / dt)
            self.last_update_time = now

    def add_file_bytes(self, file_id, byte_delta):
        """Additive variant for the multipart path (where many workers
        contribute to the same file concurrently)."""
        with self.lock:
            entry = self.file_progress.get(file_id)
            if entry is None:
                return
            entry["transferred"] += byte_delta
            now = time.time()
            dt = now - self.last_update_time
            if dt > 0 and byte_delta > 0:
                self.speed_samples.append(byte_delta / dt)
            self.last_update_time = now

    def complete_file(self, file_id):
        with self.lock:
            entry = self.file_progress.pop(file_id, None)
            if entry is not None:
                self.completed_files += 1
                self.completed_size += entry["size"]

    def skip_file(self, file_id, file_size):
        with self.lock:
            self.completed_files += 1
            self.completed_size += file_size
            self.skipped_files += 1
            self.skipped_size += file_size
            self.file_progress.pop(file_id, None)

    def get_stats(self):
        with self.lock:
            in_progress_size = sum(
                f["transferred"] for f in self.file_progress.values()
            )
            total_transferred = min(
                self.completed_size + in_progress_size, self.total_size
            )
            file_percentage = (
                self.completed_files / self.total_files * 100 if self.total_files else 0
            )
            byte_percentage = (
                total_transferred / self.total_size * 100 if self.total_size else 0
            )
            if byte_percentage >= 100.0 and self.completed_files < self.total_files:
                display_percentage = file_percentage
            else:
                display_percentage = byte_percentage

            avg_speed = (
                sum(self.speed_samples) / len(self.speed_samples)
                if self.speed_samples
                else 0
            )
            if avg_speed == 0:
                elapsed = time.time() - self.start_time
                actual_transferred = total_transferred - self.skipped_size
                avg_speed = (
                    actual_transferred / elapsed
                    if elapsed > 0 and actual_transferred > 0
                    else 0
                )

            remaining_size = self.total_size - total_transferred
            eta_seconds = (
                remaining_size / avg_speed
                if avg_speed > 0 and remaining_size > 0
                else 0
            )

            if (
                total_transferred >= self.total_size
                and self.completed_files < self.total_files
            ):
                elapsed = time.time() - self.start_time
                if elapsed > 0:
                    if self.completed_files > self.skipped_files:
                        downloaded_files = self.completed_files - self.skipped_files
                        files_per_second = (
                            downloaded_files / elapsed if downloaded_files > 0 else 0.1
                        )
                    else:
                        files_per_second = (
                            self.completed_files / elapsed
                            if self.completed_files > 0
                            else 0.1
                        )
                    remaining_files = self.total_files - self.completed_files
                    eta_seconds = (
                        remaining_files / files_per_second
                        if files_per_second > 0
                        else 0
                    )
            elif eta_seconds == 0 and self.completed_files < self.total_files:
                elapsed = time.time() - self.start_time
                if elapsed > 0 and self.completed_files > 0:
                    files_per_second = self.completed_files / elapsed
                    remaining_files = self.total_files - self.completed_files
                    eta_seconds = (
                        remaining_files / files_per_second
                        if files_per_second > 0
                        else 0
                    )

            return {
                "completed_files": self.completed_files,
                "total_files": self.total_files,
                "completed_size": total_transferred,
                "total_size": self.total_size,
                "skipped_files": self.skipped_files,
                "skipped_size": self.skipped_size,
                "downloaded_files": self.completed_files - self.skipped_files,
                "speed": avg_speed,
                "eta_seconds": eta_seconds,
                "percentage": display_percentage,
                "file_percentage": file_percentage,
                "byte_percentage": byte_percentage,
            }


# ---------------------------------------------------------------------------
# Per-file plan / sidecar handling
# ---------------------------------------------------------------------------


@dataclass
class FileTask:
    object_name: str
    final_path: str
    part_path: str
    idx_path: str
    sidecar_path: str
    size: int
    etag: str
    file_id: str = field(init=False)

    def __post_init__(self):
        self.file_id = self.object_name


def _sidecar_matches(sidecar_path: str, server_etag: str, server_size: int) -> bool:
    try:
        with open(sidecar_path) as fh:
            meta = json.load(fh)
        return (
            meta.get("etag") == server_etag and int(meta.get("size", -1)) == server_size
        )
    except (OSError, ValueError):
        return False


def _write_sidecar(sidecar_path: str, server_etag: str, server_size: int) -> None:
    tmp = sidecar_path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump({"etag": server_etag, "size": server_size}, fh)
    os.replace(tmp, sidecar_path)


def _load_idx(idx_path: str) -> set:
    try:
        with open(idx_path) as fh:
            return set(int(x) for x in json.load(fh).get("done", []))
    except (OSError, ValueError):
        return set()


def _save_idx(idx_path: str, done: set) -> None:
    tmp = idx_path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump({"done": sorted(done)}, fh)
    os.replace(tmp, idx_path)


def _allocate_part(part_path: str, size: int) -> None:
    """Create part_path as a sparse file of exactly `size` bytes.

    Uses os.open + ftruncate so behaviour is consistent across Linux / NFS
    backends and we don't depend on Python's append-mode quirks.  Existing
    contents (e.g. a previous run's partial bytes) are preserved by
    ftruncate when `size` >= current file length.
    """
    fd = os.open(part_path, os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        os.ftruncate(fd, size)
    finally:
        os.close(fd)


def _pwrite_all(fd: int, data: bytes, offset: int) -> None:
    """Write ``data`` at ``offset`` via pwrite, handling short writes.

    POSIX pwrite() can legally return fewer bytes than requested (NFS,
    EINTR, signal delivery between syscalls).  The original code assumed
    one call always wrote everything, which silently produced misaligned
    `.part` files on backends that short-write.
    """
    view = memoryview(data)
    written = 0
    while written < len(view):
        n = os.pwrite(fd, view[written:], offset + written)
        if n <= 0:
            raise IOError(
                f"pwrite returned {n} for {len(view) - written} bytes at offset {offset + written}"
            )
        written += n


# ---------------------------------------------------------------------------
# Single-file download paths
# ---------------------------------------------------------------------------


def _ranged_stream_to_fd(
    client: Minio,
    bucket: str,
    key: str,
    offset: int,
    length: int,
    write_at: int,
    fd: int,
    progress: TransferProgress,
    file_id: str,
    shutdown: threading.Event,
) -> None:
    """Download bytes [offset, offset+length) and pwrite them at `write_at`.

    Implements its own retry loop (rather than ``@tenacity.retry``) so it
    can subtract the partial bytes counted on a failed attempt from
    ``progress``.  Without this correction, every retry double-counts the
    chunks that streamed before the disconnect, inflating ETA/speed and
    pushing per-file `transferred` past the file size.
    """
    backoff = 1.0
    max_attempts = max(1, NETWORK_RETRY_ATTEMPTS)
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        if shutdown.is_set():
            raise InterruptedError("shutdown requested")
        bytes_streamed = 0
        response = None
        try:
            response = client.get_object(bucket, key, offset=offset, length=length)
            cursor = write_at
            for chunk in response.stream(STREAM_CHUNK_BYTES):
                if shutdown.is_set():
                    raise InterruptedError("shutdown requested")
                if not chunk:
                    continue
                _pwrite_all(fd, chunk, cursor)
                cursor += len(chunk)
                bytes_streamed += len(chunk)
                progress.add_file_bytes(file_id, len(chunk))
            return
        except InterruptedError:
            raise
        except _RETRYABLE_EXC as exc:
            last_exc = exc
            # Roll back this attempt's contribution to progress so the
            # retry doesn't double-count bytes that streamed before the
            # disconnect.  Speed samples only inflate on positive deltas
            # (see TransferProgress.add_file_bytes), so a negative rollback
            # corrects `transferred` without touching the EMA.
            if bytes_streamed:
                progress.add_file_bytes(file_id, -bytes_streamed)
            if attempt >= max_attempts:
                raise
            print(
                f"[retry] {key} offset={offset} attempt {attempt}/{max_attempts} "
                f"after {type(exc).__name__}: {exc}; sleeping {backoff:.1f}s"
            )
            # Honour shutdown during the backoff sleep.
            if shutdown.wait(timeout=backoff):
                raise InterruptedError("shutdown requested")
            backoff = min(backoff * 2, 16.0)
        except Exception:
            # Non-retryable: drop the bytes from progress for consistency
            # and bubble up so the caller can decide.
            if bytes_streamed:
                progress.add_file_bytes(file_id, -bytes_streamed)
            raise
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass
                try:
                    response.release_conn()
                except Exception:
                    pass

    # Defensive: should be unreachable because the loop either returns,
    # re-raises last_exc on the final attempt, or raises a non-retryable.
    if last_exc is not None:
        raise last_exc


def _resume_offset_for_singlestream(part_path: str, server_size: int) -> int:
    if not os.path.exists(part_path):
        return 0
    existing = os.path.getsize(part_path)
    if existing > server_size:
        os.remove(part_path)
        return 0
    return existing


def _download_singlestream(
    client: Minio,
    bucket: str,
    task: FileTask,
    progress: TransferProgress,
    shutdown: threading.Event,
) -> None:
    """Streaming, resumable download for files below the multipart threshold.

    Handles three regimes:

    * task.size == 0: create an empty `.part` so `_finalize` can rename it
      without crashing on a missing path.
    * resume: existing `.part` smaller than the server size → GET only the
      tail with a Range header.
    * fresh: no `.part` → GET the whole file.
    """
    progress.start_file(task.file_id, task.size)

    # Zero-byte object: just touch the .part file and we're done.  Without
    # this branch _download_one's os.path.getsize(task.part_path) below
    # raises FileNotFoundError for any empty object in the registry.
    if task.size == 0:
        os.makedirs(os.path.dirname(task.part_path), exist_ok=True)
        fd = os.open(task.part_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        os.close(fd)
        return

    offset = _resume_offset_for_singlestream(task.part_path, task.size)
    if offset > 0:
        progress.add_file_bytes(task.file_id, offset)

    if offset < task.size:
        os.makedirs(os.path.dirname(task.part_path), exist_ok=True)
        fd = os.open(task.part_path, os.O_WRONLY | os.O_CREAT, 0o644)
        try:
            _ranged_stream_to_fd(
                client,
                bucket,
                task.object_name,
                offset=offset,
                length=task.size - offset,
                write_at=offset,
                fd=fd,
                progress=progress,
                file_id=task.file_id,
                shutdown=shutdown,
            )
            os.fsync(fd)
        finally:
            os.close(fd)


def _download_multipart(
    client: Minio,
    bucket: str,
    task: FileTask,
    progress: TransferProgress,
    shutdown: threading.Event,
) -> None:
    """Parallel ranged GETs into a single pre-allocated .part file.

    We fsync the fd in the `finally:` clause so partial work done before
    a sibling worker raises is durable on disk.  Without this, a worker
    that completed its chunk (and persisted its offset to `.part.idx`)
    can have its bytes still sitting in the page cache when another
    worker fails — a subsequent node crash would then read `.idx` saying
    the offset is done while the on-disk bytes are stale.
    """
    progress.start_file(task.file_id, task.size)

    # Zero-byte object: handled by the singlestream path, but guard anyway.
    if task.size == 0:
        os.makedirs(os.path.dirname(task.part_path), exist_ok=True)
        fd = os.open(task.part_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        os.close(fd)
        return

    os.makedirs(os.path.dirname(task.part_path), exist_ok=True)
    _allocate_part(task.part_path, task.size)

    # Plan parts.
    parts: list[tuple[int, int]] = []
    offset = 0
    while offset < task.size:
        length = min(MULTIPART_PART_SIZE_BYTES, task.size - offset)
        parts.append((offset, length))
        offset += length

    # Drop any persisted offsets that don't match the current plan (e.g.
    # MULTIPART_PART_SIZE_BYTES changed between runs) so we re-fetch them.
    valid_offsets = {start for start, _ in parts}
    done = _load_idx(task.idx_path) & valid_offsets

    already = sum(length for offset, length in parts if offset in done)
    if already:
        progress.add_file_bytes(task.file_id, already)

    pending = [(o, length) for (o, length) in parts if o not in done]
    if not pending:
        return

    idx_lock = threading.Lock()
    # Signal so a single worker failure causes the others to abort
    # promptly instead of finishing pointless work.  Distinct from the
    # global shutdown event (which signals SIGTERM).
    file_abort = threading.Event()
    fd = os.open(task.part_path, os.O_WRONLY, 0o644)
    try:

        def _do_part(offset_length):
            offset, length = offset_length
            if shutdown.is_set() or file_abort.is_set():
                raise InterruptedError("shutdown or sibling-failure abort")
            try:
                _ranged_stream_to_fd(
                    client,
                    bucket,
                    task.object_name,
                    offset=offset,
                    length=length,
                    write_at=offset,
                    fd=fd,
                    progress=progress,
                    file_id=task.file_id,
                    shutdown=shutdown,
                )
            except Exception:
                file_abort.set()
                raise
            with idx_lock:
                done.add(offset)
                _save_idx(task.idx_path, done)

        workers = min(INTRAFILE_WORKERS, len(pending))
        with ThreadPoolExecutor(max_workers=workers) as inner:
            futures = [inner.submit(_do_part, p) for p in pending]
            # Drain every future so we re-raise on the first failure but
            # also wait for siblings to unwind cleanly (the abort event
            # makes them exit fast).
            first_exc: Optional[BaseException] = None
            for fut in futures:
                try:
                    fut.result()
                except BaseException as exc:
                    if first_exc is None:
                        first_exc = exc
                    file_abort.set()
            if first_exc is not None:
                raise first_exc
    finally:
        # fsync any work that landed (even if we're unwinding via an
        # exception).  Persisting now means the `.part.idx` entries that
        # were saved before the failure remain truthful for the next
        # resume attempt.
        try:
            os.fsync(fd)
        except OSError:
            pass
        os.close(fd)


def _finalize(task: FileTask) -> None:
    """Atomically promote .part → final, write etag sidecar, clean up .idx.

    We remove the `.idx` file BEFORE the rename so a crash between rename
    and sidecar-write doesn't leave an orphaned idx referencing offsets
    that no longer make sense for the finalised file.
    """
    if os.path.exists(task.idx_path):
        try:
            os.remove(task.idx_path)
        except OSError:
            pass
    os.replace(task.part_path, task.final_path)
    _write_sidecar(task.sidecar_path, task.etag, task.size)


def _maybe_skip(task: FileTask, progress: TransferProgress) -> bool:
    """Idempotency check: return True if the file is already complete on disk.

    Decision matrix when ``final_path`` exists with matching size:

    * sidecar matches server etag → skip (the canonical happy path).
    * sidecar missing → trust the size match, write the sidecar, and skip.
      This handles legacy files that pre-date the etag-tracking scheme.
    * sidecar present but etag MISMATCHES → re-download.  Treating a
      mismatch as 'just promote' would silently ship stale bytes to the
      runtime when the source object was replaced with new content that
      happens to have the same length (re-trained weights, partial
      corruption that preserved size, etc.).
    """
    if not os.path.exists(task.final_path):
        return False
    local_size = os.path.getsize(task.final_path)
    if local_size != task.size:
        return False

    if _sidecar_matches(task.sidecar_path, task.etag, task.size):
        progress.skip_file(task.file_id, task.size)
        return True

    if os.path.exists(task.sidecar_path):
        # Sidecar exists and disagrees with the server: cannot trust the
        # on-disk file.  Tear it down and let the caller re-download.
        print(f"[stale] {task.object_name}: local etag != server; re-downloading")
        for stale in (
            task.final_path,
            task.sidecar_path,
            task.part_path,
            task.idx_path,
        ):
            try:
                os.remove(stale)
            except OSError:
                pass
        return False

    # Sidecar absent — legacy or first-run promotion.  Trust size, write
    # the sidecar so future runs hit the fast path.
    try:
        _write_sidecar(task.sidecar_path, task.etag, task.size)
    except OSError:
        pass
    progress.skip_file(task.file_id, task.size)
    return True


def _download_one(
    client: Minio,
    bucket: str,
    task: FileTask,
    progress: TransferProgress,
    shutdown: threading.Event,
) -> bool:
    if shutdown.is_set():
        return False

    if _maybe_skip(task, progress):
        print(f"[skip] {task.object_name}")
        return True

    try:
        if task.size >= MULTIPART_THRESHOLD_BYTES:
            _download_multipart(client, bucket, task, progress, shutdown)
        else:
            _download_singlestream(client, bucket, task, progress, shutdown)

        if shutdown.is_set():
            return False

        # Verify on-disk size before finalising.  If it doesn't match we
        # leave the .part in place for the next run to resume from.
        on_disk = os.path.getsize(task.part_path)
        if on_disk != task.size:
            print(
                f"[warn] {task.object_name}: expected {task.size} bytes, "
                f"on disk {on_disk} — will retry on next attempt"
            )
            return False

        _finalize(task)
        progress.complete_file(task.file_id)
        print(f"[ok]   {task.object_name}")
        return True

    except InterruptedError:
        print(f"[stop] {task.object_name} interrupted; partial state preserved")
        return False
    except S3Error as err:
        print(f"[err]  {task.object_name}: S3Error {err}")
        return False
    except Exception as err:
        print(f"[err]  {task.object_name}: {err}")
        return False


# ---------------------------------------------------------------------------
# Planning + top-level entry points
# ---------------------------------------------------------------------------


def _plan_tasks(
    client: Minio,
    bucket: str,
    prefix: str,
    local_destination: str,
) -> list[FileTask]:
    objects: Iterable = client.list_objects(bucket, prefix=prefix, recursive=True)
    tasks: list[FileTask] = []
    for obj in objects:
        if obj.object_name.endswith("/"):
            continue
        relative = (
            obj.object_name[len(prefix) :]
            if obj.object_name.startswith(prefix)
            else obj.object_name
        )
        if relative.startswith("/"):
            relative = relative[1:]
        final_path = os.path.join(local_destination, relative)
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        # list_objects returns size + etag for each key — no extra HEAD needed.
        size = int(obj.size or 0)
        etag = (obj.etag or "").strip('"')
        tasks.append(
            FileTask(
                object_name=obj.object_name,
                final_path=final_path,
                part_path=final_path + ".part",
                idx_path=final_path + ".part.idx",
                sidecar_path=final_path + ".etag",
                size=size,
                etag=etag,
            )
        )
    return tasks


def _progress_loop(
    progress: TransferProgress,
    shutdown: threading.Event,
    done: threading.Event,
    operation: str,
) -> None:
    while not done.is_set() and not shutdown.is_set():
        stats = progress.get_stats()
        eta_seconds = int(stats["eta_seconds"])
        eta_min, eta_sec = divmod(eta_seconds, 60)
        files_info = f"{stats['completed_files']}/{stats['total_files']} files"
        if stats["downloaded_files"] or stats["skipped_files"]:
            files_info += (
                f" ({stats['downloaded_files']} new"
                + (
                    f", {stats['skipped_files']} skipped"
                    if stats["skipped_files"]
                    else ""
                )
                + ")"
            )
        print(
            f"[progress] {files_info} - "
            f"{stats['percentage']:.1f}% - "
            f"{stats['speed'] / 1024 / 1024:.2f} MB/s - "
            f"ETA: {eta_min}m {eta_sec}s"
        )
        update_status(
            {
                "total_files": stats["total_files"],
                "completed_files": stats["completed_files"],
                "total_size": stats["total_size"],
                "completed_size": int(stats["completed_size"]),
                "eta": eta_seconds,
                "status": "downloading" if operation == "download" else "uploading",
            }
        )
        # `wait()` returns early when shutdown fires, so SIGTERM is responsive.
        shutdown.wait(timeout=STATUS_UPDATE_INTERVAL_SECS)


def download_folder(
    client: Minio,
    bucket_name: str,
    prefix: str,
    local_destination: str,
    shutdown: Optional[threading.Event] = None,
) -> bool:
    """Download every object under `prefix` into `local_destination`.

    Idempotent and resumable: re-running against the same PVC will skip files
    that already match the server, and resume partial files from the byte
    they were last persisted at.  A `shutdown` event (set by SIGTERM) causes
    workers to unwind promptly, leaving `.part` / `.part.idx` state on disk
    for the next pod incarnation.
    """
    shutdown = shutdown or threading.Event()
    print(f"[plan] bucket={bucket_name} prefix={prefix} dest={local_destination}")

    tasks = _plan_tasks(client, bucket_name, prefix, local_destination)
    if not tasks:
        print("[plan] no objects to download")
        flush_status(
            status="completed",
            total_files=0,
            completed_files=0,
            total_size=0,
            completed_size=0,
            eta=0,
        )
        return True

    total_size = sum(t.size for t in tasks)
    # Refuse to silently "succeed" when every planned object is zero-byte
    # but there are objects listed.  This is almost always a sign of a
    # listing edge case (multipart uploads in progress, placeholder markers,
    # bad pagination) and would otherwise produce a deployment with no
    # model weights on the PVC.
    if total_size == 0 and any(t.size == 0 for t in tasks):
        zero_files = [t.object_name for t in tasks if t.size == 0]
        print(
            f"[plan] WARN: all {len(zero_files)} planned objects are zero-byte "
            f"— refusing to declare success; sample: {zero_files[:3]}"
        )
        flush_status(
            status="failed",
            total_files=len(tasks),
            completed_files=0,
            total_size=0,
            completed_size=0,
            eta=0,
            reason="all listed objects are zero-byte",
        )
        return False

    print(f"[plan] {len(tasks)} files, {total_size / 1024 / 1024:.1f} MiB total")

    progress = TransferProgress(len(tasks), total_size)
    done = threading.Event()
    progress_thread = threading.Thread(
        target=_progress_loop,
        args=(progress, shutdown, done, "download"),
        daemon=True,
    )
    progress_thread.start()

    try:
        with ThreadPoolExecutor(max_workers=OUTER_WORKERS) as executor:
            futures = [
                executor.submit(
                    _download_one, client, bucket_name, t, progress, shutdown
                )
                for t in tasks
            ]
            results = [f.result() for f in futures]
        success = all(results) and not shutdown.is_set()
    finally:
        done.set()
        progress_thread.join(timeout=2 * STATUS_UPDATE_INTERVAL_SECS)

    final_stats = progress.get_stats()
    if shutdown.is_set():
        flush_status(
            status="interrupted",
            total_files=final_stats["total_files"],
            completed_files=final_stats["completed_files"],
            total_size=final_stats["total_size"],
            completed_size=int(final_stats["completed_size"]),
            eta=int(final_stats["eta_seconds"]),
            reason="SIGTERM received",
        )
        print("[done] interrupted; partial state on disk")
        return False

    flush_status(
        status="completed" if success else "failed",
        total_files=final_stats["total_files"],
        completed_files=final_stats["completed_files"],
        total_size=final_stats["total_size"],
        completed_size=int(final_stats["completed_size"]),
        eta=0,
        **({"reason": "one or more files failed"} if not success else {}),
    )
    print("[done] " + ("ok" if success else "failed"))
    return success


# ---------------------------------------------------------------------------
# Upload (unchanged behaviour — kept for parity with previous CLI surface)
# ---------------------------------------------------------------------------


def upload_folder(
    client: Minio,
    bucket_name: str,
    prefix: str,
    local_destination: str,
    shutdown: Optional[threading.Event] = None,
) -> bool:
    shutdown = shutdown or threading.Event()
    print(f"[plan] uploading {local_destination} → s3://{bucket_name}/{prefix}")

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    upload_files = []
    for root, _, files in os.walk(local_destination):
        for fname in files:
            local_file_path = os.path.join(root, fname)
            relative_path = os.path.relpath(local_file_path, local_destination)
            object_name = os.path.join(prefix, relative_path).replace("\\", "/")
            upload_files.append(
                {
                    "file_path": local_file_path,
                    "object_name": object_name,
                    "size": os.path.getsize(local_file_path),
                }
            )

    if not upload_files:
        print("[plan] no files to upload")
        flush_status(
            status="completed",
            total_files=0,
            completed_files=0,
            total_size=0,
            completed_size=0,
            eta=0,
        )
        return True

    total_size = sum(f["size"] for f in upload_files)
    progress = TransferProgress(len(upload_files), total_size)
    done = threading.Event()
    progress_thread = threading.Thread(
        target=_progress_loop,
        args=(progress, shutdown, done, "upload"),
        daemon=True,
    )
    progress_thread.start()

    def _upload_one(info):
        if shutdown.is_set():
            return False
        try:
            try:
                stat = client.stat_object(bucket_name, info["object_name"])
                if stat.size == info["size"]:
                    progress.skip_file(info["object_name"], info["size"])
                    return True
            except S3Error:
                pass

            cb = ProgressCallback(progress, info["object_name"])
            client.fput_object(
                bucket_name,
                info["object_name"],
                info["file_path"],
                progress=cb,
            )
            progress.complete_file(info["object_name"])
            return True
        except S3Error as err:
            print(f"[err] {info['object_name']}: {err}")
            return False

    try:
        with ThreadPoolExecutor(max_workers=OUTER_WORKERS) as executor:
            results = list(executor.map(_upload_one, upload_files))
        success = all(results) and not shutdown.is_set()
    finally:
        done.set()
        progress_thread.join(timeout=2 * STATUS_UPDATE_INTERVAL_SECS)

    final_stats = progress.get_stats()
    if shutdown.is_set():
        flush_status(
            status="interrupted",
            total_files=final_stats["total_files"],
            completed_files=final_stats["completed_files"],
            total_size=final_stats["total_size"],
            completed_size=int(final_stats["completed_size"]),
            eta=int(final_stats["eta_seconds"]),
            reason="SIGTERM received",
        )
        return False

    flush_status(
        status="completed" if success else "failed",
        total_files=final_stats["total_files"],
        completed_files=final_stats["completed_files"],
        total_size=final_stats["total_size"],
        completed_size=int(final_stats["completed_size"]),
        eta=0,
    )
    return success
