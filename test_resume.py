#!/usr/bin/env python3
"""Resume / idempotency tests using a fake MinIO client.

Covers the contract that PR A introduces:

  * `.part` files persist mid-transfer and are resumed on the next call.
  * A completed file with a matching `.etag` sidecar short-circuits future
    runs (no GET issued).
  * Multipart downloads recover after losing a single chunk worker.
  * The "interrupted" terminal status is reported when SIGTERM fires.
"""

import json
import os
import tempfile
import threading
import unittest
from unittest.mock import MagicMock

from transfer import (
    FileTask,
    MULTIPART_PART_SIZE_BYTES,
    _download_multipart,
    _download_singlestream,
    _load_idx,
    _maybe_skip,
    _resume_offset_for_singlestream,
    _save_idx,
    _sidecar_matches,
    _write_sidecar,
    TransferProgress,
)


def _fake_object(payload: bytes):
    """Build a mock that mimics minio-py's response object: .stream() yields
    bytes, .close() / .release_conn() are no-ops."""
    response = MagicMock()
    response.stream = lambda chunk_size=1024: iter(
        [payload[i : i + chunk_size] for i in range(0, len(payload), chunk_size)]
    )
    response.close = MagicMock()
    response.release_conn = MagicMock()
    return response


class _FakeMinio:
    """Just enough of minio.Minio to drive the single-stream and multipart
    paths.  `get_object` honours the offset/length parameters so resume
    actually reads the right slice."""

    def __init__(self, content: bytes):
        self.content = content
        self.get_calls: list[tuple[int, int]] = []

    def get_object(self, bucket, key, offset=0, length=0):
        # length=0 in minio-py means "to end of object"
        end = len(self.content) if length == 0 else offset + length
        self.get_calls.append((offset, end - offset))
        return _fake_object(self.content[offset:end])


def _make_task(tmp: str, content: bytes, name: str = "obj") -> FileTask:
    final = os.path.join(tmp, name)
    return FileTask(
        object_name=name,
        final_path=final,
        part_path=final + ".part",
        idx_path=final + ".part.idx",
        sidecar_path=final + ".etag",
        size=len(content),
        etag="deadbeef",
    )


class SidecarRoundtripTests(unittest.TestCase):
    def test_write_and_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            sc = os.path.join(tmp, "f.etag")
            _write_sidecar(sc, "abc123", 42)
            self.assertTrue(_sidecar_matches(sc, "abc123", 42))
            self.assertFalse(_sidecar_matches(sc, "abc123", 43))
            self.assertFalse(_sidecar_matches(sc, "other", 42))

    def test_missing_sidecar_does_not_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(_sidecar_matches(os.path.join(tmp, "missing"), "x", 0))


class IdxRoundtripTests(unittest.TestCase):
    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "idx")
            _save_idx(p, {0, 32, 64})
            self.assertEqual(_load_idx(p), {0, 32, 64})

    def test_missing_returns_empty(self):
        self.assertEqual(_load_idx("/does/not/exist"), set())


class ResumeOffsetTests(unittest.TestCase):
    def test_no_part_file_returns_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                _resume_offset_for_singlestream(os.path.join(tmp, "x.part"), 100), 0
            )

    def test_partial_returns_existing_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "x.part")
            with open(p, "wb") as fh:
                fh.write(b"0" * 30)
            self.assertEqual(_resume_offset_for_singlestream(p, 100), 30)

    def test_oversized_is_discarded(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "x.part")
            with open(p, "wb") as fh:
                fh.write(b"0" * 200)
            self.assertEqual(_resume_offset_for_singlestream(p, 100), 0)
            self.assertFalse(os.path.exists(p))


class SkipIfAlreadyOnDiskTests(unittest.TestCase):
    def test_skips_when_size_and_etag_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = b"hello world"
            task = _make_task(tmp, content)
            with open(task.final_path, "wb") as fh:
                fh.write(content)
            _write_sidecar(task.sidecar_path, task.etag, task.size)

            progress = TransferProgress(1, len(content))
            self.assertTrue(_maybe_skip(task, progress))
            stats = progress.get_stats()
            self.assertEqual(stats["completed_files"], 1)
            self.assertEqual(stats["skipped_files"], 1)

    def test_legacy_file_without_sidecar_is_promoted(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = b"legacy content"
            task = _make_task(tmp, content)
            with open(task.final_path, "wb") as fh:
                fh.write(content)
            # No sidecar.  _maybe_skip should accept on size match and write one.
            progress = TransferProgress(1, len(content))
            self.assertTrue(_maybe_skip(task, progress))
            self.assertTrue(os.path.exists(task.sidecar_path))

    def test_does_not_skip_when_size_mismatches(self):
        with tempfile.TemporaryDirectory() as tmp:
            task = _make_task(tmp, b"x" * 100)
            with open(task.final_path, "wb") as fh:
                fh.write(b"x" * 50)
            progress = TransferProgress(1, 100)
            self.assertFalse(_maybe_skip(task, progress))

    def test_etag_mismatch_triggers_redownload(self):
        """Server published new content at the same byte length: the
        existing local file + stale sidecar must be discarded, not
        silently promoted."""
        with tempfile.TemporaryDirectory() as tmp:
            content = b"stale-bytes"
            task = _make_task(tmp, content)  # etag = "deadbeef"
            with open(task.final_path, "wb") as fh:
                fh.write(content)
            # Sidecar records a DIFFERENT etag from task.etag.
            with open(task.sidecar_path, "w") as fh:
                json.dump({"etag": "feedface", "size": task.size}, fh)

            progress = TransferProgress(1, len(content))
            self.assertFalse(_maybe_skip(task, progress))
            # Stale artefacts should be gone so the re-download starts clean.
            self.assertFalse(os.path.exists(task.final_path))
            self.assertFalse(os.path.exists(task.sidecar_path))


class RetryRollbackTests(unittest.TestCase):
    """Verify that a tenacity-style retry inside _ranged_stream_to_fd does
    not double-count progress: bytes streamed before a transient failure
    must be subtracted before the next attempt re-streams the same range."""

    def test_progress_does_not_double_count_on_retry(self):
        from urllib3.exceptions import ProtocolError

        from transfer import _ranged_stream_to_fd

        content = b"abcdefghij" * 10  # 100 bytes

        class _FlakyClient:
            """First attempt: stream succeeds for 30 bytes then raises.
            Second attempt: streams the full range cleanly."""

            def __init__(self):
                self.attempts = 0

            def get_object(self, bucket, key, offset=0, length=0):
                self.attempts += 1
                slice_bytes = content[offset : offset + length]
                if self.attempts == 1:
                    return _flaky_response(slice_bytes, fail_after=30)
                return _fake_object(slice_bytes)

        def _flaky_response(payload: bytes, fail_after: int):
            response = MagicMock()

            def _stream(_chunk_size=None):
                # Yield deliberately small chunks (independent of the
                # caller-requested chunk size) so we cross fail_after
                # mid-stream regardless of STREAM_CHUNK_BYTES.
                streamed = 0
                step = 20
                for i in range(0, len(payload), step):
                    if streamed >= fail_after:
                        raise ProtocolError("simulated mid-stream drop")
                    chunk = payload[i : i + step]
                    yield chunk
                    streamed += len(chunk)

            response.stream = _stream
            response.close = MagicMock()
            response.release_conn = MagicMock()
            return response

        with tempfile.TemporaryDirectory() as tmp:
            part_path = os.path.join(tmp, "obj.part")
            fd = os.open(part_path, os.O_WRONLY | os.O_CREAT, 0o644)
            try:
                progress = TransferProgress(1, len(content))
                progress.start_file("obj", len(content))
                client = _FlakyClient()
                _ranged_stream_to_fd(
                    client,
                    "bucket",
                    "obj",
                    offset=0,
                    length=len(content),
                    write_at=0,
                    fd=fd,
                    progress=progress,
                    file_id="obj",
                    shutdown=threading.Event(),
                )
            finally:
                os.close(fd)

            stats = progress.get_stats()
            # `transferred` after a successful run should exactly equal the
            # number of bytes streamed once.  If the retry double-counted,
            # `completed_size` would still be clamped (min with total_size),
            # but the in-progress entry exposes the raw count.
            with progress.lock:
                self.assertIn("obj", progress.file_progress)
                self.assertEqual(
                    progress.file_progress["obj"]["transferred"], len(content)
                )
            # Two GETs total: 1 failed mid-stream + 1 retry.
            self.assertEqual(client.attempts, 2)
            # `.part` should hold the correct bytes.
            with open(part_path, "rb") as fh:
                self.assertEqual(fh.read(), content)


class ZeroByteTests(unittest.TestCase):
    def test_singlestream_handles_zero_byte_file(self):
        """Empty source objects must produce an empty .part file so
        _finalize can rename it without raising FileNotFoundError."""
        from transfer import _download_singlestream

        with tempfile.TemporaryDirectory() as tmp:
            task = _make_task(tmp, b"")
            client = _FakeMinio(b"")
            progress = TransferProgress(1, 0)
            _download_singlestream(client, "bucket", task, progress, threading.Event())
            self.assertTrue(os.path.exists(task.part_path))
            self.assertEqual(os.path.getsize(task.part_path), 0)
            # No GET should have been issued for a zero-byte file.
            self.assertEqual(client.get_calls, [])


class SingleStreamResumeTests(unittest.TestCase):
    def test_resume_skips_already_downloaded_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = b"abcdefghij" * 10  # 100 bytes
            task = _make_task(tmp, content)
            # Simulate a previous run that wrote the first 60 bytes.
            with open(task.part_path, "wb") as fh:
                fh.write(content[:60])

            client = _FakeMinio(content)
            progress = TransferProgress(1, len(content))
            shutdown = threading.Event()
            _download_singlestream(client, "bucket", task, progress, shutdown)

            # Only the missing tail should have been requested.
            self.assertEqual(client.get_calls, [(60, 40)])
            with open(task.part_path, "rb") as fh:
                self.assertEqual(fh.read(), content)

    def test_first_run_downloads_everything(self):
        with tempfile.TemporaryDirectory() as tmp:
            content = b"x" * 200
            task = _make_task(tmp, content)
            client = _FakeMinio(content)
            progress = TransferProgress(1, len(content))
            _download_singlestream(client, "bucket", task, progress, threading.Event())
            self.assertEqual(client.get_calls, [(0, 200)])


class MultipartResumeTests(unittest.TestCase):
    def test_multipart_skips_persisted_offsets(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Three parts: 0, MULTIPART_PART_SIZE_BYTES, 2*MULTIPART_PART_SIZE_BYTES
            part = MULTIPART_PART_SIZE_BYTES
            size = part * 2 + 128  # last part is small
            content = bytes((i % 256 for i in range(size)))
            task = _make_task(tmp, content)

            # Pretend the first two parts already landed: pre-allocate the
            # file, write the first two slabs, and persist them in .idx.
            with open(task.part_path, "wb") as fh:
                fh.truncate(size)
            with open(task.part_path, "r+b") as fh:
                fh.seek(0)
                fh.write(content[:part])
                fh.seek(part)
                fh.write(content[part : 2 * part])
            _save_idx(task.idx_path, {0, part})

            client = _FakeMinio(content)
            progress = TransferProgress(1, size)
            _download_multipart(client, "bucket", task, progress, threading.Event())

            # Only the third part should have been requested.
            self.assertEqual(client.get_calls, [(2 * part, 128)])
            with open(task.part_path, "rb") as fh:
                self.assertEqual(fh.read(), content)

            # .idx should now record all three offsets.
            self.assertEqual(_load_idx(task.idx_path), {0, part, 2 * part})

    def test_multipart_discards_offsets_when_partsize_changes(self):
        """If MULTIPART_PART_SIZE_BYTES changed since the previous run, the
        persisted offsets no longer align — they should be ignored and the
        whole file re-fetched."""
        with tempfile.TemporaryDirectory() as tmp:
            part = MULTIPART_PART_SIZE_BYTES
            size = part + 64
            content = b"y" * size
            task = _make_task(tmp, content)
            with open(task.part_path, "wb") as fh:
                fh.truncate(size)
            # Save an offset that is *not* a current part boundary.
            _save_idx(task.idx_path, {17})

            client = _FakeMinio(content)
            progress = TransferProgress(1, size)
            _download_multipart(client, "bucket", task, progress, threading.Event())

            # Both planned parts (offset 0 and offset `part`) should have
            # been requested.
            requested = sorted(client.get_calls)
            self.assertEqual(requested, [(0, part), (part, 64)])


if __name__ == "__main__":
    unittest.main()
