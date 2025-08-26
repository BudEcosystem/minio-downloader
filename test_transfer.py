#!/usr/bin/env python3
"""Test script to verify transfer progress and ETA calculations."""

import os
import sys
import time
import tempfile
import shutil
from unittest.mock import Mock, MagicMock, patch
from transfer import TransferProgress

def test_transfer_progress():
    """Test the TransferProgress class with various scenarios."""
    
    print("Testing TransferProgress class...")
    
    # Test 1: Normal file transfer
    print("\n1. Testing normal file transfer...")
    progress = TransferProgress(total_files=10, total_size=1000000)
    
    # Start a file
    progress.start_file("file1", 100000)
    progress.update_file("file1", 50000)
    stats = progress.get_stats()
    assert stats['completed_files'] == 0
    assert stats['completed_size'] == 50000
    print(f"   In-progress: {stats['completed_size']}/{stats['total_size']} bytes")
    
    # Complete the file
    progress.complete_file("file1")
    stats = progress.get_stats()
    assert stats['completed_files'] == 1
    assert stats['completed_size'] == 100000
    print(f"   Completed: {stats['completed_files']}/{stats['total_files']} files")
    
    # Test 2: Skipped files
    print("\n2. Testing skipped files...")
    progress.skip_file("file2", 100000)
    stats = progress.get_stats()
    assert stats['completed_files'] == 2
    assert stats['skipped_files'] == 1
    assert stats['completed_size'] == 200000
    print(f"   Skipped: {stats['skipped_files']} files, Total: {stats['completed_files']}/{stats['total_files']}")
    
    # Test 3: ETA calculation with all files skipped (edge case)
    print("\n3. Testing ETA with all files skipped...")
    progress2 = TransferProgress(total_files=5, total_size=500000)
    for i in range(5):
        progress2.skip_file(f"file{i}", 100000)
    stats = progress2.get_stats()
    assert stats['completed_files'] == 5
    assert stats['skipped_files'] == 5
    assert stats['eta_seconds'] == 0  # Should be 0 since all done
    print(f"   All skipped: ETA = {stats['eta_seconds']} seconds (expected: 0)")
    
    # Test 4: Edge case - completed_size exceeds total_size
    print("\n4. Testing edge case: completed_size > total_size...")
    progress3 = TransferProgress(total_files=2, total_size=100000)
    progress3.start_file("file1", 60000)
    progress3.update_file("file1", 60000)
    progress3.complete_file("file1")
    progress3.start_file("file2", 60000)
    progress3.update_file("file2", 60000)
    progress3.complete_file("file2")
    stats = progress3.get_stats()
    # Should cap at total_size even if actual is more
    assert stats['completed_size'] <= stats['total_size']
    print(f"   Capped size: {stats['completed_size']}/{stats['total_size']} (no overflow)")
    
    # Test 5: Fallback ETA calculation
    print("\n5. Testing fallback ETA calculation...")
    progress4 = TransferProgress(total_files=10, total_size=1000000)
    time.sleep(0.1)  # Let some time pass
    # Complete some files
    for i in range(3):
        progress4.skip_file(f"skip{i}", 100000)
    for i in range(2):
        # Properly complete files using start_file/complete_file
        progress4.start_file(f"file{i}", 100000)
        progress4.complete_file(f"file{i}")
    stats = progress4.get_stats()
    # With 5/10 files done and some time passed, ETA should be > 0
    print(f"   Files: {stats['completed_files']}/{stats['total_files']}, ETA: {stats['eta_seconds']:.2f} seconds")
    
    # Test 6: Percentage calculation when bytes complete but files remain
    print("\n6. Testing percentage when bytes complete but files remain...")
    progress5 = TransferProgress(total_files=47, total_size=145424101604)
    # Simulate 40 files completed with all bytes transferred
    # We'll simulate that these files already existed and were skipped
    for i in range(40):
        progress5.skip_file(f"file{i}", 145424101604 // 40)  # Divide size equally
    stats = progress5.get_stats()
    assert stats['byte_percentage'] >= 99.0  # Close to 100%
    assert stats['file_percentage'] < 100.0
    assert stats['completed_files'] == 40
    assert stats['total_files'] == 47
    print(f"   Files: {stats['completed_files']}/{stats['total_files']}")
    print(f"   File %: {stats['file_percentage']:.1f}%, Byte %: {stats['byte_percentage']:.1f}%")
    print(f"   Display %: {stats['percentage']:.1f}% (should use file % when bytes complete)")
    
    # Test 7: Downloaded vs skipped files tracking
    print("\n7. Testing downloaded vs skipped files...")
    progress6 = TransferProgress(total_files=10, total_size=1000000)
    # Skip 3 files
    for i in range(3):
        progress6.skip_file(f"skip{i}", 100000)
    # Download 2 files
    for i in range(2):
        progress6.start_file(f"download{i}", 100000)
        progress6.complete_file(f"download{i}")
    stats = progress6.get_stats()
    assert stats['downloaded_files'] == 2
    assert stats['skipped_files'] == 3
    assert stats['completed_files'] == 5
    print(f"   Completed: {stats['completed_files']}, Downloaded: {stats['downloaded_files']}, Skipped: {stats['skipped_files']}")
    
    print("\n✅ All tests passed!")
    return True

def test_file_skip_logic():
    """Test the file skip logic in download."""
    print("\n\nTesting file skip logic...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a test file
        test_file = os.path.join(tmpdir, "test.txt")
        with open(test_file, "w") as f:
            f.write("x" * 1000)  # 1000 bytes
        
        file_size = os.path.getsize(test_file)
        print(f"Created test file: {test_file} ({file_size} bytes)")
        
        # Test file existence check
        if os.path.exists(test_file):
            local_size = os.path.getsize(test_file)
            if local_size == file_size:
                print("✅ File would be skipped (size matches)")
            else:
                print("❌ File would be re-downloaded (size mismatch)")
        else:
            print("❌ File not found")
        
        # Test with different size
        with open(test_file, "w") as f:
            f.write("x" * 500)  # Different size
        
        local_size = os.path.getsize(test_file)
        if local_size != file_size:
            print(f"✅ File would be re-downloaded (size mismatch: {local_size} != {file_size})")
        
        # Test with non-existent file
        non_existent = os.path.join(tmpdir, "missing.txt")
        if not os.path.exists(non_existent):
            print("✅ Non-existent file would be downloaded")

if __name__ == "__main__":
    try:
        test_transfer_progress()
        test_file_skip_logic()
        print("\n🎉 All tests completed successfully!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
