# Technical Specification: Persistent File Hash Caching via Extended Attributes

## 1. Overview

This document outlines a standardized approach for caching cryptographic file hashes using filesystem Extended Attributes (`xattrs`).

By storing the hash and the modification time (`mtime`) directly in the file's metadata, applications can avoid expensive re-computation of file hashes on subsequent runs. This mechanism is designed for Linux-based systems running on modern filesystems (ext4, btrfs, xfs, zfs).

## 2. Attribute Schema

To ensure interoperability between different tools, the following naming conventions and value formats must be used.

### 2.1 Namespace

All attributes must be stored in the `user` namespace. This allows unprivileged processes (running as the file owner) to read and write these values.

**Root Namespace:** `user.checksum`

### 2.2 defined Attributes

| Attribute Key | Value Format | Description |
| --- | --- | --- |
| `user.checksum.<algo>` | Hexadecimal String | The computed hash of the file content. `<algo>` denotes the algorithm used (e.g., `sha256`, `blake3`). |
| `user.checksum.mtime` | ASCII Integer | The Unix timestamp (seconds since epoch) of the file at the time the hash was computed. |

**Example:**

```text
user.checksum.sha256="5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
user.checksum.mtime="1707145821"

```

### 2.3 Value Encoding

* **Hashes:** Must be stored as **lowercase hexadecimal strings**. Do not store raw binary bytes to ensure `getfattr` readability and avoid null-byte termination issues in some C-string handling libraries.
* **Time:** Must be stored as a string representation of the integer seconds (e.g., `"1707145821"`). Nanoseconds are excluded to avoid precision mismatches between different stat system calls or languages.

## 3. Implementation Logic

Integrators must implement the following logic flows to ensure cache consistency.

### 3.1 Reading the Hash (The "Get" Operation)

When an application needs the hash of a file:

1. **Stat the File:** Retrieve the current modification time (`current_mtime`) of the file from the OS.
2. **Fetch Attributes:** Attempt to read `user.checksum.mtime` and `user.checksum.<algo>`.
3. **Validate Cache:**
* If attributes are missing: **Cache Miss**.
* If `user.checksum.mtime` != `current_mtime`: **Cache Miss** (File has changed since last hash).
* If `user.checksum.mtime` == `current_mtime`: **Cache Hit**.


4. **Return:** On a hit, return the stored hash immediately. On a miss, proceed to "Writing the Hash".

### 3.2 Writing the Hash (The "Set" Operation)

When an application computes a hash (due to a cache miss):

1. **Pre-Calculation Stat:** Record the `mtime` of the file (*t1*) before opening it.
2. **Compute:** Read the file stream and calculate the hash.
3. **Post-Calculation Stat:** Record the `mtime` of the file (*t2*) after closing it.
4. **Atomicity Check:**
* If *t1* != *t2*: **Abort**. The file was modified while being read. Do not write cache; returning the computed hash is risky as it represents a transient state.


5. **Write Attributes:** If *t1* == *t2*, write both attributes:
* `user.checksum.<algo>` = computed_hash
* `user.checksum.mtime` = *t2*



## 4. Operational Constraints & Edge Cases

### 4.1 Permission Denied

Writing to `user.*` attributes requires the process to verify ownership or write permissions on the target file.

* **Behavior:** If `setxattr` fails with `EPERM` or `EACCES`, the application **must** fail silently regarding the cache (log a debug warning) and return the computed hash. The application should not crash due to a lack of caching ability.

### 4.2 Filesystem Support

Not all locations support xattrs (e.g., `/tmp` on tmpfs, FAT32 mounts, some NFS configurations).

* **Behavior:** Treat `EOPNOTSUPP` (Operation not supported) as a permanent Cache Miss for that specific file.

### 4.3 Nanosecond Precision

This specification deliberately ignores nanosecond precision for `mtime`.

* **Reasoning:** Many tools (like `touch` or archive extractors) only restore mtime to the nearest second. Relying on nanoseconds increases the rate of false-negative cache misses without significantly improving consistency for this use case.

## 5. Reference Implementation (Pseudo-Code)

```python
def get_file_hash(filepath):
    # 1. Get current file state
    current_stat = os.stat(filepath)
    current_mtime = str(int(current_stat.st_mtime))

    # 2. Try Cache Lookup
    try:
        cached_mtime = xattr.get(filepath, "user.checksum.mtime")
        cached_hash = xattr.get(filepath, "user.checksum.sha256")
        
        if cached_mtime == current_mtime:
            return cached_hash
    except IOException:
        pass # Attribute missing or FS not supported

    # 3. Cache Miss - Recompute
    # Capture state before read to ensure atomicity
    pre_stat = os.stat(filepath)
    
    computed_hash = heavy_compute_hash(filepath)
    
    post_stat = os.stat(filepath)

    # 4. Write Cache (Only if file wasn't touched during read)
    if pre_stat.st_mtime == post_stat.st_mtime:
        try:
            xattr.set(filepath, "user.checksum.sha256", computed_hash)
            xattr.set(filepath, "user.checksum.mtime", str(int(post_stat.st_mtime)))
        except IOException:
            pass # Fail silently if FS doesn't support xattr

    return computed_hash

```