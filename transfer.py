import os
import time
from minio import Minio
from minio.error import S3Error
from typing import List
from concurrent.futures import ThreadPoolExecutor
import threading

from status import update_status

def download_folder(
    client: Minio, 
    bucket_name: str, 
    prefix: str, 
    local_destination: str
):
    """
    Downloads all objects in `bucket_name` with the given `prefix`,
    preserving the same folder structure under `local_destination`.
    
    :param client:            Initialized Minio client.
    :param bucket_name:       Name of the bucket to download from.
    :param prefix:            The prefix (folder path) in the bucket to download.
                            For example, "myfolder/subfolder/".
    :param local_destination: Local directory where you want to store the downloaded files.
    """
    print(f"Starting download from bucket: {bucket_name}, prefix: {prefix}")
    print(f"Downloading to: {local_destination}")
    
    # List all objects under the specified prefix
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    objects_list = list(objects)  # Convert iterator to list to check if empty

    download_files = []
    
    if not objects_list:
        print(f"No objects found in bucket {bucket_name} with prefix {prefix}")
        return
    
    for obj in objects_list:
        relative_path = obj.object_name[len(prefix):] if obj.object_name.startswith(prefix) else obj.object_name
        
        # Skip if this is a directory marker
        if obj.object_name.endswith('/'):
            continue
        if relative_path.startswith("/"):
            relative_path = relative_path[1:]
            
        # Construct the full local file path
        local_file_path = os.path.join(local_destination, relative_path)
        
        # Create any necessary directories
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        # Download the object to the local file path
        try:
            # client.fget_object(bucket_name, obj.object_name, local_file_path)
            download_files.append({
                'file_path': local_file_path,
                'object_name': obj.object_name
            })
            # print(f"Successfully downloaded: {obj.object_name} -> {local_file_path}")
        except S3Error as err:
            print(f"Error downloading {obj.object_name}: {err}")
    
    if not download_files:
        print("No files to download")
        return True
    
    # Set up progress tracking
    total_files = len(download_files)
    completed_files = 0
    lock = threading.Lock()
    start_time = time.time()

    progress_configmap = {
        "total_files": str(total_files),
        "completed_files": str(completed_files),
        "eta": "",
        "status": "downloading"
    }

    def download_file(file_info):
        nonlocal completed_files
        try:
            client.fget_object(bucket_name, file_info['object_name'], file_info['file_path'])

            with lock:
                completed_files += 1
                elapsed_time = time.time() - start_time
                files_per_second = completed_files / elapsed_time if elapsed_time > 0 else 0
                remaining_files = total_files - completed_files
                eta_seconds = remaining_files / files_per_second if files_per_second > 0 else 0

                eta_min = int(eta_seconds // 60)
                eta_sec = int(eta_seconds % 60)

                print(f"Progress: {completed_files}/{total_files} files " 
                      f"({(completed_files/total_files)*100:.1f}%) - "
                      f"ETA: {eta_min}m {eta_sec}s - "
                      f"Downloaded: {file_info['object_name']}")
                progress_configmap["completed_files"] = str(completed_files)
                progress_configmap["eta"] = str(eta_seconds)
                update_status(progress_configmap)
            return True
        except S3Error as err:
            print(f"Error downloading {file_info['object_name']}: {err}")
            progress_configmap["status"] = "failed"
            update_status(progress_configmap)
            return False
        
    # Use ThreadPoolExecutor for parallel uploads
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(download_file, download_files))
    
    success = all(results)
    
    if success:
        print("Download completed successfully!")
        progress_configmap["status"] = "completed"
        update_status(progress_configmap)
        return True
    else:
        print(f"Failed to download {prefix} from store://{bucket_name}/{prefix}")
        progress_configmap["status"] = "failed"
        update_status(progress_configmap)
        return False


def upload_folder(client: Minio, bucket_name: str, prefix: str, local_destination: str):
    """
    Upload all files from local_destination to MinIO bucket with the given prefix.
    Uses parallel uploads and tracks progress to calculate ETA.
    """
    
    print(f"Uploading from: {local_destination} to store://{bucket_name}/{prefix}")
    
    # Ensure bucket exists
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    
    # Collect all files to upload
    upload_files = []
    for root, _, files in os.walk(local_destination):
        for file in files:
            local_file_path = os.path.join(root, file)
            
            # Calculate the relative path from local_destination
            relative_path = os.path.relpath(local_file_path, local_destination)
            
            # Construct the object name in the bucket
            object_name = os.path.join(prefix, relative_path).replace('\\', '/')
            
            upload_files.append({
                'file_path': local_file_path,
                'object_name': object_name
            })
    
    if not upload_files:
        print("No files to upload")
        return True
    
    print(f"Found {len(upload_files)} files to upload")
    
    # Set up progress tracking
    total_files = len(upload_files)
    completed_files = 0
    lock = threading.Lock()
    start_time = time.time()

    progress_configmap = {
        "total_files": str(total_files),
        "completed_files": str(completed_files),
        "eta": "",
        "status": "uploading"
    }
    
    def upload_file(file_info):
        nonlocal completed_files
        try:
            client.fput_object(
                bucket_name, 
                file_info['object_name'], 
                file_info['file_path']
            )
            
            with lock:
                completed_files += 1
                elapsed_time = time.time() - start_time
                files_per_second = completed_files / elapsed_time if elapsed_time > 0 else 0
                remaining_files = total_files - completed_files
                eta_seconds = remaining_files / files_per_second if files_per_second > 0 else 0
                
                eta_min = int(eta_seconds // 60)
                eta_sec = int(eta_seconds % 60)
                
                print(f"Progress: {completed_files}/{total_files} files " 
                      f"({(completed_files/total_files)*100:.1f}%) - "
                      f"ETA: {eta_min}m {eta_sec}s - "
                      f"Uploaded: {file_info['object_name']}")
                progress_configmap["completed_files"] = str(completed_files)
                progress_configmap["eta"] = str(eta_seconds)
                update_status(progress_configmap)
            return True
        except S3Error as err:
            print(f"Error uploading {file_info['object_name']}: {err}")
            progress_configmap["status"] = "failed"
            update_status(progress_configmap)
            return False
    
    # Use ThreadPoolExecutor for parallel uploads
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(upload_file, upload_files))
    
    success = all(results)
    
    if success:
        print("Upload completed successfully!")
        progress_configmap["status"] = "completed"
        update_status(progress_configmap)
        return True
    else:
        print(f"Failed to upload some files to store://{bucket_name}/{prefix}")
        progress_configmap["status"] = "failed"
        update_status(progress_configmap)
        return False
