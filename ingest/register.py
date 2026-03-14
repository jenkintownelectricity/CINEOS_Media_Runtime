"""
CINEOS Media Runtime — Media object registration after upload.

Responsibilities:
  1. Compute SHA-256 checksum of uploaded content.
  2. Derive content-addressed media_id from the checksum.
  3. Check for duplicates (same hash = same media).
  4. Register the MediaObject in the database.
  5. Emit a ``media.registered`` CloudEvent.
"""

from __future__ import annotations

import datetime
from typing import Any, BinaryIO, Optional, Protocol, Union

from ingest.checksum import compute_sha256
from ingest.events import CloudEvent, EventEmitter, media_registered
from ingest.models import (
    IngestStatus,
    MediaObject,
    MediaType,
    RegistrationResult,
)


# ---------------------------------------------------------------------------
# Media database protocol
# ---------------------------------------------------------------------------

class MediaDatabase(Protocol):
    """Protocol for the persistence layer that stores MediaObjects."""

    async def get_by_media_id(self, tenant_id: str, media_id: str) -> Optional[MediaObject]:
        """Return an existing media object or None."""
        ...

    async def insert(self, media_object: MediaObject) -> None:
        """Persist a new media object."""
        ...


# ---------------------------------------------------------------------------
# In-memory database for testing
# ---------------------------------------------------------------------------

class InMemoryMediaDatabase:
    """Simple in-memory store keyed by (tenant_id, media_id)."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], MediaObject] = {}

    async def get_by_media_id(self, tenant_id: str, media_id: str) -> Optional[MediaObject]:
        return self._store.get((tenant_id, media_id))

    async def insert(self, media_object: MediaObject) -> None:
        key = (media_object.tenant_id, media_object.media_id)
        self._store[key] = media_object

    @property
    def count(self) -> int:
        return len(self._store)


# ---------------------------------------------------------------------------
# Registration logic
# ---------------------------------------------------------------------------

async def register_media(
    tenant_id: str,
    source: Union[str, bytes, BinaryIO],
    *,
    filename: str,
    content_type: str,
    media_type: MediaType,
    size_bytes: int,
    storage_bucket: str,
    storage_key: str,
    metadata: dict[str, Any] | None = None,
    database: MediaDatabase,
    emitter: EventEmitter,
) -> RegistrationResult:
    """
    Register a media object after upload.

    Steps:
      1. Compute the SHA-256 checksum of the source content.
      2. Use the checksum as the content-addressed ``media_id``.
      3. Check for an existing record with the same ``(tenant_id, media_id)``.
         - If found, return it as a duplicate (no re-insert, no re-emit).
      4. Create and persist a new ``MediaObject``.
      5. Emit a ``media.registered`` CloudEvent.

    Parameters
    ----------
    tenant_id
        Owning tenant.
    source
        File path, raw bytes, or readable binary stream for checksum computation.
    filename, content_type, media_type, size_bytes
        Descriptive metadata about the uploaded file.
    storage_bucket, storage_key
        Object-store location of the uploaded file.
    metadata
        Optional freeform metadata.
    database
        Persistence backend.
    emitter
        Event emitter for CloudEvents.

    Returns
    -------
    RegistrationResult
    """
    # Step 1 + 2: compute content-addressed ID
    checksum = compute_sha256(source)
    media_id = checksum

    # Step 3: duplicate check
    existing = await database.get_by_media_id(tenant_id, media_id)
    if existing is not None:
        return RegistrationResult(
            media_id=media_id,
            tenant_id=tenant_id,
            is_duplicate=True,
            media_object=existing,
        )

    # Step 4: create and persist
    now = datetime.datetime.now(datetime.timezone.utc)
    media_object = MediaObject(
        media_id=media_id,
        tenant_id=tenant_id,
        filename=filename,
        content_type=content_type,
        media_type=media_type,
        size_bytes=size_bytes,
        storage_bucket=storage_bucket,
        storage_key=storage_key,
        checksum_sha256=checksum,
        metadata=metadata or {},
        created_at=now,
        updated_at=now,
    )
    await database.insert(media_object)

    # Step 5: emit event
    event = media_registered(
        tenant_id=tenant_id,
        media_id=media_id,
        filename=filename,
        content_type=content_type,
        size_bytes=size_bytes,
        is_duplicate=False,
    )
    await emitter.emit(event)

    return RegistrationResult(
        media_id=media_id,
        tenant_id=tenant_id,
        is_duplicate=False,
        media_object=media_object,
    )
