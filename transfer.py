import os
import time
from minio import Minio
from minio.error import S3Error
from typing import List
from concurrent.futures import ThreadPoolExecutor
import threading
from collections import deque
from datetime import datetime

from status import update_status


class ProgressCallback(threading.Thread):
    """Progress callback for MinIO file operations."""
    
    def __init__(self, transfer_progress, file_id):
        threading.Thread.__init__(self)
        self.daemon = True
        self.transfer_progress = transfer_progress
        self.file_id = file_id
        self.total_length = 0
        self.current_size = 0
        
    def set_meta(self, total_length, object_name):
        """Called by MinIO client to set file metadata."""
        self.total_length = total_length
        self.object_name = object_name
        self.transfer_progress.start_file(self.file_id, total_length)
        
    def update(self, size):
        """Called by MinIO client to update progress."""
        self.current_size += size
        self.transfer_progress.update_file(self.file_id, self.current_size)
        
    def run(self):
        """Thread run method (required by MinIO client)."""
        pass


class TransferProgress:
    """Track progress for parallel file transfers with accurate ETA calculation."""
    
    def __init__(self, total_files, total_size):
        self.total_files = total_files
        self.total_size = total_size
        self.completed_files = 0
        self.completed_size = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
        
        # Track progress for each file
        self.file_progress = {}
        
        # Rolling window for speed calculation (last 10 updates)
        self.speed_samples = deque(maxlen=10)
        self.last_update_time = self.start_time
        
    def start_file(self, file_id, file_size):
        """Mark a file as started."""
        with self.lock:
            self.file_progress[file_id] = {
                'size': file_size,
                'transferred': 0,
                'start_time': time.time()
            }
    
    def update_file(self, file_id, bytes_transferred):
        """Update progress for a specific file."""
        with self.lock:
            if file_id in self.file_progress:
                old_transferred = self.file_progress[file_id]['transferred']
                self.file_progress[file_id]['transferred'] = bytes_transferred
                
                # Calculate speed based on this update
                current_time = time.time()
                time_delta = current_time - self.last_update_time
                if time_delta > 0:
                    bytes_delta = bytes_transferred - old_transferred
                    speed = bytes_delta / time_delta
                    self.speed_samples.append(speed)
                
                self.last_update_time = current_time
    
    def complete_file(self, file_id):
        """Mark a file as completed."""
        with self.lock:
            if file_id in self.file_progress:
                file_info = self.file_progress[file_id]
                self.completed_files += 1
                self.completed_size += file_info['size']
                del self.file_progress[file_id]
    
    def get_stats(self):
        """Get current transfer statistics."""
        with self.lock:
            # Calculate total transferred (completed + in-progress)
            in_progress_size = sum(f['transferred'] for f in self.file_progress.values())
            total_transferred = self.completed_size + in_progress_size
            
            # Calculate average speed from recent samples
            avg_speed = sum(self.speed_samples) / len(self.speed_samples) if self.speed_samples else 0
            
            # If no recent samples, use overall average
            if avg_speed == 0:
                elapsed = time.time() - self.start_time
                avg_speed = total_transferred / elapsed if elapsed > 0 else 0
            
            # Calculate ETA
            remaining_size = self.total_size - total_transferred
            eta_seconds = remaining_size / avg_speed if avg_speed > 0 else 0
            
            return {
                'completed_files': self.completed_files,
                'total_files': self.total_files,
                'completed_size': total_transferred,
                'total_size': self.total_size,
                'speed': avg_speed,
                'eta_seconds': eta_seconds,
                'percentage': (total_transferred / self.total_size * 100) if self.total_size > 0 else 0
            }

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
    
    # Get total size of all files
    total_size = 0
    for file_info in download_files:
        try:
            stat = client.stat_object(bucket_name, file_info['object_name'])
            file_info['size'] = stat.size
            total_size += stat.size
        except S3Error as err:
            print(f"Error getting size for {file_info['object_name']}: {err}")
            return False
    
    # Initialize progress tracking
    progress = TransferProgress(len(download_files), total_size)
    
    # Progress display thread
    def display_progress():
        """Display progress updates periodically."""
        while True:
            stats = progress.get_stats()
            if stats['completed_files'] == stats['total_files']:
                break
                
            eta_seconds = stats['eta_seconds']
            eta_min = int(eta_seconds // 60)
            eta_sec = int(eta_seconds % 60)
            
            print(f"Progress: {stats['completed_files']}/{stats['total_files']} files "
                  f"({stats['percentage']:.1f}%) - "
                  f"Speed: {stats['speed']/1024/1024:.2f} MB/s - "
                  f"ETA: {eta_min}m {eta_sec}s")
            
            # Update Kubernetes ConfigMap
            progress_configmap = {
                "total_files": str(stats['total_files']),
                "completed_files": str(stats['completed_files']),
                "total_size": str(stats['total_size']),
                "completed_size": str(int(stats['completed_size'])),
                "eta": str(eta_seconds),
                "status": "downloading"
            }
            update_status(progress_configmap)
            
            time.sleep(1)
    
    # Start progress display thread
    progress_thread = threading.Thread(target=display_progress)
    progress_thread.daemon = True
    progress_thread.start()

    def download_file(file_info):
        file_id = file_info['object_name']
        try:
            # Create progress callback for this file
            progress_callback = ProgressCallback(progress, file_id)
            
            # Download with progress tracking
            client.fget_object(
                bucket_name, 
                file_info['object_name'], 
                file_info['file_path'],
                progress=progress_callback
            )
            
            # Mark file as completed
            progress.complete_file(file_id)
            print(f"Downloaded: {file_info['object_name']}")
            return True
            
        except S3Error as err:
            print(f"Error downloading {file_info['object_name']}: {err}")
            return False
        
    # Use ThreadPoolExecutor for parallel downloads
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(download_file, download_files))
    
    success = all(results)
    
    # Final status update
    final_stats = progress.get_stats()
    progress_configmap = {
        "total_files": str(final_stats['total_files']),
        "completed_files": str(final_stats['completed_files']),
        "total_size": str(final_stats['total_size']),
        "completed_size": str(int(final_stats['completed_size'])),
        "eta": "0",
        "status": "completed" if success else "failed"
    }
    update_status(progress_configmap)
    
    if success:
        print("Download completed successfully!")
        return True
    else:
        print(f"Failed to download some files from store://{bucket_name}/{prefix}")
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
    
    # Get total size of all files
    total_size = 0
    for file_info in upload_files:
        try:
            file_size = os.path.getsize(file_info['file_path'])
            file_info['size'] = file_size
            total_size += file_size
        except OSError as err:
            print(f"Error getting size for {file_info['file_path']}: {err}")
            return False
    
    # Initialize progress tracking
    progress = TransferProgress(len(upload_files), total_size)
    
    # Progress display thread
    def display_progress():
        """Display progress updates periodically."""
        while True:
            stats = progress.get_stats()
            if stats['completed_files'] == stats['total_files']:
                break
                
            eta_seconds = stats['eta_seconds']
            eta_min = int(eta_seconds // 60)
            eta_sec = int(eta_seconds % 60)
            
            print(f"Progress: {stats['completed_files']}/{stats['total_files']} files "
                  f"({stats['percentage']:.1f}%) - "
                  f"Speed: {stats['speed']/1024/1024:.2f} MB/s - "
                  f"ETA: {eta_min}m {eta_sec}s")
            
            # Update Kubernetes ConfigMap
            progress_configmap = {
                "total_files": str(stats['total_files']),
                "completed_files": str(stats['completed_files']),
                "total_size": str(stats['total_size']),
                "completed_size": str(int(stats['completed_size'])),
                "eta": str(eta_seconds),
                "status": "uploading"
            }
            update_status(progress_configmap)
            
            time.sleep(1)
    
    # Start progress display thread
    progress_thread = threading.Thread(target=display_progress)
    progress_thread.daemon = True
    progress_thread.start()
    
    def upload_file(file_info):
        file_id = file_info['object_name']
        try:
            # Create progress callback for this file
            progress_callback = ProgressCallback(progress, file_id)
            
            # Upload with progress tracking
            client.fput_object(
                bucket_name, 
                file_info['object_name'], 
                file_info['file_path'],
                progress=progress_callback
            )
            
            # Mark file as completed
            progress.complete_file(file_id)
            print(f"Uploaded: {file_info['object_name']}")
            return True
            
        except S3Error as err:
            print(f"Error uploading {file_info['object_name']}: {err}")
            return False
    
    # Use ThreadPoolExecutor for parallel uploads
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(upload_file, upload_files))
    
    success = all(results)
    
    # Final status update
    final_stats = progress.get_stats()
    progress_configmap = {
        "total_files": str(final_stats['total_files']),
        "completed_files": str(final_stats['completed_files']),
        "total_size": str(final_stats['total_size']),
        "completed_size": str(int(final_stats['completed_size'])),
        "eta": "0",
        "status": "completed" if success else "failed"
    }
    update_status(progress_configmap)
    
    if success:
        print("Upload completed successfully!")
        return True
    else:
        print(f"Failed to upload some files to store://{bucket_name}/{prefix}")
        return False
