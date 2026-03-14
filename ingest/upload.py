"""
CINEOS Media Runtime — Signed upload URL initiation.

Generates pre-signed S3-style URLs for direct media uploads.
All operations are tenant-scoped.
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any, Optional, Protocol

from ingest.models import UploadRequest, UploadURL


# ---------------------------------------------------------------------------
# Storage backend protocol
# ---------------------------------------------------------------------------

class StorageBackend(Protocol):
    """Protocol for object-store backends that can issue pre-signed upload URLs."""

    async def generate_presigned_upload(
        self,
        bucket: str,
        key: str,
        content_type: str,
        size_bytes: int,
        expires_in_seconds: int,
    ) -> str:
        """Return a pre-signed URL string."""
        ...


# ---------------------------------------------------------------------------
# Default / local storage backend (for testing and local dev)
# ---------------------------------------------------------------------------

class LocalStorageBackend:
    """
    Local/stub storage backend that produces deterministic fake URLs.
    Suitable for tests and local development.
    """

    def __init__(self, base_url: str = "https://storage.cineos.local") -> None:
        self.base_url = base_url.rstrip("/")

    async def generate_presigned_upload(
        self,
        bucket: str,
        key: str,
        content_type: str,
        size_bytes: int,
        expires_in_seconds: int,
    ) -> str:
        token = uuid.uuid4().hex[:16]
        return f"{self.base_url}/{bucket}/{key}?X-Upload-Token={token}&expires={expires_in_seconds}"


# ---------------------------------------------------------------------------
# Upload URL generator
# ---------------------------------------------------------------------------

# Default expiry: 1 hour
DEFAULT_EXPIRY_SECONDS: int = 3600
DEFAULT_BUCKET: str = "cineos-media"


def _storage_key(tenant_id: str, upload_id: str, filename: str) -> str:
    """Build a deterministic, tenant-scoped storage key."""
    # Sanitise filename (keep basename only)
    safe_name = filename.replace("/", "_").replace("\\", "_")
    return f"tenants/{tenant_id}/uploads/{upload_id}/{safe_name}"


async def initiate_upload(
    request: UploadRequest,
    storage: StorageBackend,
    *,
    bucket: str = DEFAULT_BUCKET,
    expires_in_seconds: int = DEFAULT_EXPIRY_SECONDS,
) -> UploadURL:
    """
    Generate a pre-signed upload URL for the client to PUT their media file.

    Parameters
    ----------
    request
        The upload request containing tenant, filename, content type, etc.
    storage
        Storage backend capable of producing pre-signed URLs.
    bucket
        Target storage bucket.
    expires_in_seconds
        URL validity window.

    Returns
    -------
    UploadURL
        A response containing the signed URL and associated metadata.
    """
    upload_id = str(uuid.uuid4())
    storage_key = _storage_key(request.tenant_id, upload_id, request.filename)

    url = await storage.generate_presigned_upload(
        bucket=bucket,
        key=storage_key,
        content_type=request.content_type,
        size_bytes=request.size_bytes,
        expires_in_seconds=expires_in_seconds,
    )

    now = datetime.datetime.now(datetime.timezone.utc)
    expires_at = now + datetime.timedelta(seconds=expires_in_seconds)

    return UploadURL(
        upload_id=upload_id,
        tenant_id=request.tenant_id,
        url=url,
        method="PUT",
        headers={
            "Content-Type": request.content_type,
            "Content-Length": str(request.size_bytes),
        },
        expires_at=expires_at,
        storage_bucket=bucket,
        storage_key=storage_key,
    )
