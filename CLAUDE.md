# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a MinIO file transfer tool designed to download and upload models between MinIO object storage and local filesystem. The tool supports parallel transfers with progress tracking and optional Kubernetes ConfigMap status updates.

## Key Commands

### Running the Application

```bash
# Download files from MinIO to local storage
python main.py --operation download --model-path models/ --local-path /data/models-registry

# Upload files from local storage to MinIO
python main.py --operation upload --model-path models/ --local-path /data/models-registry

# With Kubernetes status updates
python main.py --operation download --model-path models/ --local-path /data/models-registry --use-kubernetes --namespace default --configmap-name transfer-progress
```

### Docker

```bash
# Build image
docker build -t minio-downloader .

# Run container
docker run -e MINIO_ENDPOINT=minio.example.com -e MINIO_ACCESS_KEY=minio -e MINIO_SECRET_KEY=minio123 -e MINIO_BUCKET=models -v /data/models-registry:/data/models-registry minio-downloader --operation download --model-path models/ --local-path /data/models-registry
```

## Architecture

### Core Components

1. **main.py**: Entry point that handles argument parsing and initializes MinIO client. Sets up environment variables for Kubernetes integration.

2. **transfer.py**: Contains the main transfer logic:
   - `download_folder()`: Downloads files from MinIO to local filesystem with parallel processing
   - `upload_folder()`: Uploads files from local filesystem to MinIO with parallel processing
   - Both functions use ThreadPoolExecutor with 10 workers for concurrent transfers
   - Progress tracking calculates ETA based on bytes transferred

3. **status.py**: Handles Kubernetes ConfigMap updates for progress tracking:
   - `update_status()`: Updates or creates ConfigMap when running in Kubernetes mode
   - ConfigMap contains: total_files, completed_files, total_size, completed_size, eta, status

4. **kube.py**: Kubernetes-specific functionality (if present)

### Key Design Decisions

- Uses concurrent transfers (10 workers) for performance
- Progress tracking includes file count and byte-level metrics
- Kubernetes integration is optional via --use-kubernetes flag
- Environment variables control MinIO connection settings
- Preserves directory structure during transfers
- Handles SSL/TLS connections via MINIO_SECURE environment variable

## Environment Variables

Required:
- `MINIO_ENDPOINT`: MinIO server endpoint
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key

Optional:
- `MINIO_BUCKET`: Bucket name (defaults to "models")
- `MINIO_SECURE`: Use HTTPS connection ("true"/"false", defaults to "false")
- `USE_KUBERNETES`: Set by --use-kubernetes flag
- `NAMESPACE`: Kubernetes namespace for ConfigMap
- `CONFIGMAP_NAME`: Name of ConfigMap for progress tracking