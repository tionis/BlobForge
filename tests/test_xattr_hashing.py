import os
import hashlib
import time
import xattr
from blobforge.utils import compute_sha256_with_cache, get_cached_hash, XATTR_HASH_KEY, XATTR_MTIME_KEY

def test_xattr_caching():
    test_file = "test_cache.pdf"
    content = b"%PDF-1.4\\ntest content"
    # Actually let's just use a simple string and encode it
    content = b"%PDF-1.4 test content"
    with open(test_file, "wb") as f:
        f.write(content)
    
    try:
        # 1. First computation - should set xattrs
        print("Computing hash for the first time...")
        h1 = compute_sha256_with_cache(test_file)
        expected_hash = hashlib.sha256(content).hexdigest()
        assert h1 == expected_hash
        
        # Check if xattrs are set
        attrs = xattr.listxattr(test_file)
        print(f"Xattrs set: {attrs}")
        assert XATTR_HASH_KEY in attrs
        assert XATTR_MTIME_KEY in attrs
        
        cached_hash = xattr.getxattr(test_file, XATTR_HASH_KEY).decode('utf-8')
        cached_mtime = xattr.getxattr(test_file, XATTR_MTIME_KEY).decode('utf-8')
        print(f"Cached hash: {cached_hash}")
        print(f"Cached mtime: {cached_mtime}")
        assert cached_hash == h1
        
        # 2. Second computation - should use cache
        print("Computing hash for the second time (should hit cache)...")
        h2 = compute_sha256_with_cache(test_file)
        assert h2 == h1
        
        # 3. Modify file - should invalidate cache
        print("Modifying file...")
        time.sleep(1.1) # Ensure mtime changes (seconds precision)
        with open(test_file, "ab") as f:
            f.write(b"modified")
        
        new_content = content + b"modified"
        new_expected_hash = hashlib.sha256(new_content).hexdigest()
        
        print("Computing hash after modification...")
        h3 = compute_sha256_with_cache(test_file)
        assert h3 == new_expected_hash
        assert h3 != h1
        
        # Check if xattrs are updated
        new_cached_hash = xattr.getxattr(test_file, XATTR_HASH_KEY).decode('utf-8')
        assert new_cached_hash == h3
        
        print("Test passed!")
        
    finally:
        if os.path.exists(test_file):
            os.remove(test_file)

if __name__ == "__main__":
    test_xattr_caching()