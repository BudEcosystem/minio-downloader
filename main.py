import argparse
import os
import signal
import sys
import threading

from minio import Minio

from transfer import download_folder, upload_folder


def _install_signal_handlers(shutdown: threading.Event) -> None:
    """SIGTERM / SIGINT set `shutdown` so workers unwind without losing
    `.part`/`.idx` state on disk; the next pod incarnation resumes from there.
    """

    def _on_signal(signum, frame):
        if shutdown.is_set():
            return
        print(f"[signal] received {signal.Signals(signum).name}, draining...")
        shutdown.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)


def main(args: argparse.Namespace) -> int:
    minio_endpoint = os.environ.get("MINIO_ENDPOINT")
    minio_secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY")
    minio_bucket = os.environ.get("MINIO_BUCKET", "models")

    if not minio_endpoint:
        print("[fatal] MINIO_ENDPOINT is required", file=sys.stderr)
        return 2
    if not minio_access_key or not minio_secret_key:
        print(
            "[fatal] MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required",
            file=sys.stderr,
        )
        return 2

    model_path = args.model_path.rstrip("/") + "/"
    local_path = args.local_path

    # status.py reads these to decide whether (and where) to write progress.
    os.environ["USE_KUBERNETES"] = "true" if args.use_kubernetes else "false"
    os.environ["NAMESPACE"] = args.namespace
    os.environ["CONFIGMAP_NAME"] = args.configmap_name

    client = Minio(
        minio_endpoint,
        cert_check=False,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure,
    )

    bucket_name = minio_bucket
    prefix = model_path
    local_destination = os.path.join(local_path, model_path)

    shutdown = threading.Event()
    _install_signal_handlers(shutdown)

    if args.operation == "download":
        success = download_folder(
            client, bucket_name, prefix, local_destination, shutdown=shutdown
        )
    elif args.operation == "upload":
        success = upload_folder(
            client, bucket_name, prefix, local_destination, shutdown=shutdown
        )
    else:
        print(f"[fatal] unknown operation: {args.operation}", file=sys.stderr)
        return 2

    if shutdown.is_set():
        return 130  # conventional exit code for SIGINT-style termination
    return 0 if success else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MinIO model operations")
    parser.add_argument(
        "--operation",
        choices=["download", "upload"],
        default="download",
        help="Operation to perform: download or upload models",
    )
    parser.add_argument(
        "--model-path", type=str, default="", help="Path to the model in the bucket"
    )
    parser.add_argument(
        "--use-kubernetes",
        action="store_true",
        help="Write progress to a Kubernetes ConfigMap",
    )
    parser.add_argument(
        "--namespace", type=str, default="default", help="Namespace to use"
    )
    parser.add_argument(
        "--configmap-name",
        type=str,
        default="transfer-progress",
        help="ConfigMap name to use",
    )
    parser.add_argument(
        "--local-path",
        type=str,
        default="/data/models-registry",
        help="Local path to use",
    )
    args = parser.parse_args()

    print(f"{args.operation.capitalize()}ing models from/to MinIO")
    sys.exit(main(args))
