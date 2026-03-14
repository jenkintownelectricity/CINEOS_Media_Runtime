"""
CINEOS Media Runtime — Pydantic models for media objects.

MediaObject identity is CONTENT-ADDRESSED: media_id = SHA-256 hash of file content.
All operations are tenant-scoped.
"""

from __future__ import annotations

import datetime
import enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class MediaType(str, enum.Enum):
    """Supported media content types."""
    VIDEO = "video"
    AUDIO = "audio"
    IMAGE = "image"
    SUBTITLE = "subtitle"
    DCP = "dcp"  # Digital Cinema Package
    OTHER = "other"


class IngestStatus(str, enum.Enum):
    """Lifecycle status of a media ingest operation."""
    UPLOAD_INITIATED = "upload_initiated"
    UPLOAD_COMPLETED = "upload_completed"
    CHECKSUM_COMPUTING = "checksum_computing"
    CHECKSUM_VALIDATED = "checksum_validated"
    REGISTERING = "registering"
    REGISTERED = "registered"
    CREATING_DERIVATIVES = "creating_derivatives"
    COMPLETED = "completed"
    FAILED = "failed"
    DUPLICATE = "duplicate"


class MediaObject(BaseModel):
    """
    Core media object record.

    The media_id is the SHA-256 hex digest of the file content,
    making it a content-addressed identifier. Two files with
    identical bytes will always produce the same media_id.
    """
    media_id: str = Field(
        ...,
        description="SHA-256 hex digest of the file content (content-addressed ID)",
        pattern=r"^[0-9a-f]{64}$",
    )
    tenant_id: str = Field(..., description="Tenant that owns this media object")
    filename: str = Field(..., description="Original upload filename")
    content_type: str = Field(..., description="MIME type of the media file")
    media_type: MediaType = Field(..., description="High-level media category")
    size_bytes: int = Field(..., ge=0, description="File size in bytes")
    storage_bucket: str = Field(..., description="Object-store bucket name")
    storage_key: str = Field(..., description="Object-store key / path")
    checksum_sha256: str = Field(
        ...,
        description="SHA-256 hex digest (same value as media_id)",
        pattern=r"^[0-9a-f]{64}$",
    )
    metadata: dict[str, Any] = Field(default_factory=dict, description="Arbitrary metadata")
    created_at: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc),
    )
    updated_at: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc),
    )

    class Config:
        frozen = True


class UploadRequest(BaseModel):
    """Request to initiate a signed upload."""
    tenant_id: str
    filename: str
    content_type: str
    media_type: MediaType
    size_bytes: int = Field(..., ge=1)
    metadata: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: Optional[str] = Field(
        default=None,
        description="Client-supplied idempotency key for retry safety",
    )


class UploadURL(BaseModel):
    """Response containing a pre-signed upload URL."""
    upload_id: str = Field(..., description="Server-generated upload identifier")
    tenant_id: str
    url: str = Field(..., description="Pre-signed upload URL")
    method: str = Field(default="PUT", description="HTTP method to use")
    headers: dict[str, str] = Field(default_factory=dict, description="Required request headers")
    expires_at: datetime.datetime
    storage_bucket: str
    storage_key: str


class RegistrationResult(BaseModel):
    """Result of media registration."""
    media_id: str
    tenant_id: str
    is_duplicate: bool = Field(
        default=False,
        description="True if an identical file was already registered",
    )
    media_object: MediaObject
    registered_at: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc),
    )


class DerivativeJob(BaseModel):
    """A job to create a derivative of a media object (e.g. transcode, thumbnail)."""
    job_id: str
    tenant_id: str
    media_id: str
    derivative_type: str = Field(..., description="E.g. 'thumbnail', 'proxy', 'transcode_h265'")
    parameters: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc),
    )


class IngestRecord(BaseModel):
    """
    Full record tracking an ingest workflow run.
    Supports idempotency via idempotency_key.
    """
    ingest_id: str
    tenant_id: str
    idempotency_key: Optional[str] = None
    upload_id: Optional[str] = None
    media_id: Optional[str] = None
    status: IngestStatus = IngestStatus.UPLOAD_INITIATED
    filename: str = ""
    content_type: str = ""
    media_type: MediaType = MediaType.OTHER
    size_bytes: int = 0
    error_message: Optional[str] = None
    created_at: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc),
    )
    updated_at: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc),
    )
