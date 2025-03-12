import os
import argparse
from minio import Minio

from transfer import download_folder, upload_folder

def main(args):
    
    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
    MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
    MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
    MINIO_BUCKET = os.environ.get("MINIO_BUCKET")
    MODEL_PATH = args.model_path + "/"
    LOCAL_PATH = args.local_path

    # Set kubernetes config in env
    os.environ["USE_KUBERNETES"] = str(args.use_kubernetes)
    os.environ["NAMESPACE"] = args.namespace
    os.environ["CONFIGMAP_NAME"] = args.configmap_name

    # Initialize MinIO client
    client = Minio(MINIO_ENDPOINT, cert_check=False,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

    # Set up paths
    bucket_name = MINIO_BUCKET
    prefix = MODEL_PATH
    local_destination = os.path.join(LOCAL_PATH, MODEL_PATH)

    if args.operation == "download":
        # Download everything under `prefix` to `local_destination`
        download_folder(client, bucket_name, prefix, local_destination)
    elif args.operation == "upload":
        # Upload everything under `local_destination` to `prefix` in `bucket_name`
        upload_folder(client, bucket_name, prefix, local_destination, args.use_kubernetes, args.namespace, args.configmap_name)


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="MinIO model operations")
    parser.add_argument("--operation", choices=["download", "upload"], default="download",
                        help="Operation to perform: download or upload models")
    parser.add_argument("--model-path", type=str, default="",
                        help="Path to the model in the bucket")
    parser.add_argument("--use-kubernetes", action="store_true", help="Use Kubernetes to update the status")
    parser.add_argument("--namespace", type=str, default="default", help="Namespace to use")
    parser.add_argument("--configmap-name", type=str, default="transfer-progress", help="ConfigMap name to use")
    parser.add_argument("--local-path", type=str, default="/data/models-registry", help="Local path to use")
    args = parser.parse_args()
    
    print(f"{args.operation.capitalize()}ing models from/to MinIO")
    
    main(args)