"""
CINEOS Media Runtime — Checksum computation and validation.

Computes SHA-256 checksums for media files. The resulting hex digest
serves as the content-addressed media_id.
"""

from __future__ import annotations

import hashlib
import io
from typing import BinaryIO, Union

# 8 MiB read buffer — balances memory usage vs. syscall overhead for large media files.
DEFAULT_CHUNK_SIZE: int = 8 * 1024 * 1024


def compute_sha256(
    source: Union[str, bytes, BinaryIO],
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> str:
    """
    Compute the SHA-256 hex digest of *source*.

    Parameters
    ----------
    source
        - ``str``: treated as a filesystem path; the file is read in chunks.
        - ``bytes``: hashed directly.
        - file-like (``BinaryIO``): read in chunks from the current position.
    chunk_size
        Read buffer size in bytes (only relevant for path / stream sources).

    Returns
    -------
    str
        Lowercase hex digest (64 characters).
    """
    hasher = hashlib.sha256()

    if isinstance(source, bytes):
        hasher.update(source)
    elif isinstance(source, str):
        # source is a file path
        with open(source, "rb") as fh:
            _feed_stream(hasher, fh, chunk_size)
    else:
        # assume file-like / BinaryIO
        _feed_stream(hasher, source, chunk_size)

    return hasher.hexdigest()


def compute_sha256_bytes(data: bytes) -> str:
    """Convenience: hash raw bytes and return hex digest."""
    return hashlib.sha256(data).hexdigest()


def validate_checksum(
    source: Union[str, bytes, BinaryIO],
    expected_sha256: str,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> bool:
    """
    Validate that *source* matches the *expected_sha256* digest.

    Returns ``True`` if the computed digest matches (case-insensitive comparison).
    """
    actual = compute_sha256(source, chunk_size=chunk_size)
    return actual.lower() == expected_sha256.lower()


class ChecksumMismatchError(Exception):
    """Raised when a computed checksum does not match the expected value."""

    def __init__(self, expected: str, actual: str) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Checksum mismatch: expected {expected}, got {actual}"
        )


def require_checksum(
    source: Union[str, bytes, BinaryIO],
    expected_sha256: str,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> str:
    """
    Compute and validate the SHA-256 digest of *source*.

    Returns the computed hex digest on success.
    Raises ``ChecksumMismatchError`` on failure.
    """
    actual = compute_sha256(source, chunk_size=chunk_size)
    if actual.lower() != expected_sha256.lower():
        raise ChecksumMismatchError(expected=expected_sha256, actual=actual)
    return actual


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _feed_stream(hasher: "hashlib._Hash", stream: BinaryIO, chunk_size: int) -> None:
    """Read *stream* in chunks and feed each chunk to *hasher*."""
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        hasher.update(chunk)
