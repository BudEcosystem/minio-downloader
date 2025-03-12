# minio-downloader

A tool to download and upload models from MinIO to a local path.

## Usage

```bash
python main.py --operation download --model-path models/ --local-path /data/models-registry
```

### Arguments

- `--operation`: Specifies the operation to perform. Options are:
  - `download`: Download files from MinIO to local storage
  - `upload`: Upload files from local storage to MinIO

- `--model-path`: The path in the MinIO bucket. For downloads, this is the source path. For uploads, this is the destination path.

- `--local-path`: The path on the local filesystem. For downloads, this is the destination path. For uploads, this is the source path.

- `--use-kubernetes`: Use Kubernetes to update the status.

- `--namespace`: The namespace to use.

- `--configmap-name`: The name of the ConfigMap to use.

### Environment variables

- `MINIO_ENDPOINT`: The MinIO server endpoint.

- `MINIO_ACCESS_KEY`: The MinIO access key.

- `MINIO_SECRET_KEY`: The MinIO secret key.

- `MINIO_BUCKET`: The MinIO bucket name. Defaults to "models" if not set.
