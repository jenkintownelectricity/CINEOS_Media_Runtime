"""
CINEOS Media Runtime — Media Ingest Package.

Content-addressed media ingest and registration flow for the
CINEOS cinema operating system. All operations are tenant-scoped.

Modules:
    models      Pydantic data models for media objects
    checksum    SHA-256 checksum computation and validation
    upload      Pre-signed upload URL generation
    register    Media object registration with duplicate detection
    events      CloudEvents emission for ingest lifecycle
    workflow    Idempotent end-to-end ingest orchestration
"""

from ingest.checksum import (
    ChecksumMismatchError,
    compute_sha256,
    compute_sha256_bytes,
    require_checksum,
    validate_checksum,
)
from ingest.events import (
    CloudEvent,
    EventEmitter,
    EventType,
    InMemoryEventEmitter,
    ingest_completed,
    ingest_failed,
    media_registered,
    upload_completed,
    upload_initiated,
)
from ingest.models import (
    DerivativeJob,
    IngestRecord,
    IngestStatus,
    MediaObject,
    MediaType,
    RegistrationResult,
    UploadRequest,
    UploadURL,
)
from ingest.register import (
    InMemoryMediaDatabase,
    MediaDatabase,
    register_media,
)
from ingest.upload import (
    LocalStorageBackend,
    StorageBackend,
    initiate_upload,
)
from ingest.workflow import (
    DefaultDerivativeJobCreator,
    IngestWorkflow,
    InMemoryIdempotencyStore,
)

__all__ = [
    # models
    "MediaObject",
    "MediaType",
    "IngestStatus",
    "UploadRequest",
    "UploadURL",
    "RegistrationResult",
    "DerivativeJob",
    "IngestRecord",
    # checksum
    "compute_sha256",
    "compute_sha256_bytes",
    "validate_checksum",
    "require_checksum",
    "ChecksumMismatchError",
    # upload
    "StorageBackend",
    "LocalStorageBackend",
    "initiate_upload",
    # register
    "MediaDatabase",
    "InMemoryMediaDatabase",
    "register_media",
    # events
    "CloudEvent",
    "EventType",
    "EventEmitter",
    "InMemoryEventEmitter",
    "upload_initiated",
    "upload_completed",
    "media_registered",
    "ingest_completed",
    "ingest_failed",
    # workflow
    "IngestWorkflow",
    "InMemoryIdempotencyStore",
    "DefaultDerivativeJobCreator",
]
