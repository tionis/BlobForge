"""
BlobForge Utilities - Common helper functions.

Provides:
- S3 metadata encoding/decoding (ASCII-safe)
- Other shared utilities
"""
import urllib.parse
from typing import Dict, Optional


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
