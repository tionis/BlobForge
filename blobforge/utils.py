"""
BlobForge Utilities - Common helper functions.

Provides:
- S3 metadata encoding/decoding (ASCII-safe)
- File hash caching via extended attributes (xattrs)
- Other shared utilities
"""
import os
import hashlib
import urllib.parse
from typing import Dict, Optional

# xattr support is optional - works on Linux/macOS with ext4, btrfs, xfs, zfs, etc.
try:
    import xattr
    XATTR_AVAILABLE = True
except ImportError:
    XATTR_AVAILABLE = False

# Extended attribute keys following the specification in docs/file_hashing_via_xattrs.md
XATTR_HASH_KEY = "user.checksum.sha256"
XATTR_MTIME_KEY = "user.checksum.mtime"


def sanitize_metadata_value(value: str) -> str:
    """
    Encode a string for use as S3 metadata value.
    
    S3 metadata can only contain ASCII characters. This function URL-encodes
    any non-ASCII characters to make the value safe for S3 storage.
    
    Args:
        value: The original string value (may contain Unicode)
        
    Returns:
        ASCII-safe string suitable for S3 metadata
    
    Example:
        >>> sanitize_metadata_value("Anna's Archive")  # curly apostrophe
        'Anna%E2%80%99s%20Archive'
    """
    if not value:
        return value
    
    # URL-encode the value, which handles non-ASCII characters
    # safe='' means encode everything except alphanumerics
    # We use a more permissive safe set to keep common ASCII chars readable
    return urllib.parse.quote(value, safe='')


def decode_metadata_value(value: str) -> str:
    """
    Decode an S3 metadata value back to the original string.
    
    Reverses the encoding done by sanitize_metadata_value().
    
    Args:
        value: The encoded metadata value from S3
        
    Returns:
        Original string with Unicode characters restored
        
    Example:
        >>> decode_metadata_value('Anna%E2%80%99s%20Archive')
        "Anna's Archive"
    """
    if not value:
        return value
    
    try:
        return urllib.parse.unquote(value)
    except Exception:
        # If decoding fails, return original value
        return value


def sanitize_metadata(metadata: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """
    Sanitize all values in a metadata dictionary for S3 storage.
    
    Args:
        metadata: Dictionary of metadata key-value pairs
        
    Returns:
        New dictionary with all values encoded as ASCII-safe strings
    """
    if not metadata:
        return metadata
    
    return {k: sanitize_metadata_value(str(v)) for k, v in metadata.items()}


def decode_metadata(metadata: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """
    Decode all values in a metadata dictionary retrieved from S3.
    
    Args:
        metadata: Dictionary of metadata key-value pairs from S3
        
    Returns:
        New dictionary with all values decoded to original Unicode strings
    """
    if not metadata:
        return metadata
    
    return {k: decode_metadata_value(v) for k, v in metadata.items()}


def safe_filename(filename: str) -> str:
    """
    Create a safe filename by removing/replacing problematic characters.
    
    This is useful for display purposes or when the filename needs to be
    used in contexts where special characters might cause issues.
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename safe for most filesystems
    """
    if not filename:
        return filename
    
    # Replace common problematic characters
    replacements = {
        '/': '_',
        '\\': '_',
        ':': '_',
        '*': '_',
        '?': '_',
        '"': '_',
        '<': '_',
        '>': '_',
        '|': '_',
        '\0': '_',
    }
    
    result = filename
    for char, replacement in replacements.items():
        result = result.replace(char, replacement)
    
    return result


def get_cached_hash(filepath: str, algorithm: str = "sha256") -> Optional[str]:
    """
    Retrieve a cached file hash from extended attributes if available and valid.
    
    Follows the specification in docs/file_hashing_via_xattrs.md:
    1. Get current file mtime
    2. Read cached mtime and hash from xattrs
    3. Validate cached mtime matches current mtime
    4. Return hash on cache hit, None on cache miss
    
    Args:
        filepath: Path to the file to check
        algorithm: Hash algorithm (currently only 'sha256' supported)
        
    Returns:
        Cached hash if valid, None if cache miss or unavailable
    """
    if not XATTR_AVAILABLE:
        return None
    
    if algorithm != "sha256":
        return None
    
    try:
        # 1. Get current file state
        current_stat = os.stat(filepath)
        current_mtime = str(int(current_stat.st_mtime))
        
        # 2. Fetch cached attributes
        cached_mtime = xattr.getxattr(filepath, XATTR_MTIME_KEY).decode('utf-8')
        cached_hash = xattr.getxattr(filepath, XATTR_HASH_KEY).decode('utf-8')
        
        # 3. Validate cache
        if cached_mtime == current_mtime:
            # Cache hit - validate hash format (lowercase hex, 64 chars for sha256)
            if len(cached_hash) == 64 and all(c in '0123456789abcdef' for c in cached_hash):
                return cached_hash
        
        # Cache miss - mtime mismatch means file changed since hash was computed
        return None
        
    except (OSError, IOError, KeyError):
        # Attribute missing, permission denied, or FS doesn't support xattr
        return None


def set_cached_hash(filepath: str, file_hash: str, algorithm: str = "sha256", mtime: Optional[float] = None) -> bool:
    """
    Store a computed file hash in extended attributes for future caching.
    
    Follows the specification in docs/file_hashing_via_xattrs.md:
    - Only stores if the file wasn't modified during hash computation
    - Fails silently if xattrs not supported or permission denied
    
    Args:
        filepath: Path to the file
        file_hash: The computed hash value (lowercase hex)
        algorithm: Hash algorithm (currently only 'sha256' supported)
        mtime: Optional timestamp to store. If None, current file mtime is used.
               Providing this is recommended to avoid race conditions.
        
    Returns:
        True if successfully cached, False otherwise
    """
    if not XATTR_AVAILABLE:
        return False
    
    if algorithm != "sha256":
        return False
    
    try:
        if mtime is None:
            # Get current mtime to store alongside the hash
            current_stat = os.stat(filepath)
            current_mtime_val = current_stat.st_mtime
        else:
            current_mtime_val = mtime
            
        current_mtime_str = str(int(current_mtime_val))
        
        # Write both attributes
        xattr.setxattr(filepath, XATTR_HASH_KEY, file_hash.encode('utf-8'))
        xattr.setxattr(filepath, XATTR_MTIME_KEY, current_mtime_str.encode('utf-8'))
        
        return True
        
    except (OSError, IOError):
        # Permission denied (EPERM/EACCES) or FS doesn't support xattr (EOPNOTSUPP)
        # Fail silently as per specification
        return False


def compute_sha256_with_cache(filepath: str) -> str:
    """
    Compute SHA256 hash of a file, using xattr cache when available.
    
    This is the main entry point for hash computation that implements
    the full caching logic from docs/file_hashing_via_xattrs.md:
    
    1. Check for valid cached hash (via get_cached_hash)
    2. On cache miss, compute hash with atomicity checks
    3. Store computed hash in cache (via set_cached_hash)
    
    Args:
        filepath: Path to the file to hash
        
    Returns:
        SHA256 hash as lowercase hexadecimal string
    """
    # 1. Try cache lookup first
    cached = get_cached_hash(filepath)
    if cached:
        return cached
    
    # 2. Cache miss - compute hash with atomicity check
    # Record mtime before reading to ensure file wasn't modified during computation
    try:
        pre_stat = os.stat(filepath)
        pre_mtime = pre_stat.st_mtime
    except OSError:
        pre_mtime = None
    
    # Compute the hash
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(65536), b""):  # 64KB blocks
            sha256_hash.update(byte_block)
    computed_hash = sha256_hash.hexdigest()
    
    # 3. Post-calculation check and cache write
    try:
        post_stat = os.stat(filepath)
        post_mtime = post_stat.st_mtime
        
        # Only cache if file wasn't modified during read (atomicity check)
        if pre_mtime is not None and pre_mtime == post_mtime:
            set_cached_hash(filepath, computed_hash, mtime=post_mtime)
    except OSError:
        pass  # Can't verify atomicity, skip caching
    
    return computed_hash
