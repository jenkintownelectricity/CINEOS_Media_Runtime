"""
CINEOS Media Runtime — Transcode Models

Pydantic models for derivative specifications, results, and job tracking.
All derivative naming is deterministic: derived from source content hash + transform parameters.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class MediaType(str, Enum):
    """Supported media types for derivative generation."""

    VIDEO = "video"
    AUDIO = "audio"
    IMAGE = "image"


class TranscodeStatus(str, Enum):
    """Lifecycle states for a transcode job."""

    PENDING = "pending"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"  # already exists (idempotent)


class DerivativeSpec(BaseModel):
    """
    Specification for a single derivative to be generated.

    The output filename is deterministic:
        {source_hash}_{transform_id}.{ext}
    This guarantees idempotency — re-running the same spec against
    the same source will always target the same output path.
    """

    source_hash: str = Field(
        ..., description="Content-addressable hash of the source media"
    )
    transform_id: str = Field(
        ..., description="Identifier of the transform profile (e.g. proxy_720p)"
    )
    media_type: MediaType
    output_format: str = Field(
        ..., description="Output container/format extension (e.g. mp4, aac, png)"
    )
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Transform parameters (resolution, codec, bitrate, etc.)",
    )
    tenant_id: str = Field(..., description="Tenant scope for multi-tenancy isolation")

    @property
    def output_filename(self) -> str:
        """Deterministic output filename derived from source hash + transform."""
        return f"{self.source_hash}_{self.transform_id}.{self.output_format}"

    @property
    def param_hash(self) -> str:
        """Hash of the transform parameters for cache-busting on param changes."""
        canonical = json.dumps(self.params, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:12]

    @field_validator("source_hash")
    @classmethod
    def validate_source_hash(cls, v: str) -> str:
        if not v or len(v) < 8:
            raise ValueError("source_hash must be at least 8 characters")
        return v


class DerivativeResult(BaseModel):
    """Result of a derivative generation operation."""

    spec: DerivativeSpec
    status: TranscodeStatus
    output_path: Optional[str] = None
    output_size_bytes: Optional[int] = None
    output_hash: Optional[str] = Field(
        None, description="Content hash of the generated derivative"
    )
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def succeeded(self) -> bool:
        return self.status == TranscodeStatus.COMPLETED


class TranscodeJob(BaseModel):
    """
    A transcode job tracks one or more derivative generation tasks
    for a single source asset, scoped to a tenant.
    """

    job_id: UUID = Field(default_factory=uuid4)
    tenant_id: str
    source_path: str
    source_hash: str
    derivatives: list[DerivativeSpec] = Field(default_factory=list)
    results: list[DerivativeResult] = Field(default_factory=list)
    status: TranscodeStatus = TranscodeStatus.PENDING
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count: int = 0
    max_retries: int = 3

    @property
    def is_retryable(self) -> bool:
        return self.status == TranscodeStatus.FAILED and self.retry_count < self.max_retries

    def mark_started(self) -> None:
        self.status = TranscodeStatus.STARTED
        self.updated_at = datetime.now(timezone.utc)

    def mark_completed(self) -> None:
        self.status = TranscodeStatus.COMPLETED
        self.updated_at = datetime.now(timezone.utc)

    def mark_failed(self, error: Optional[str] = None) -> None:
        self.status = TranscodeStatus.FAILED
        self.retry_count += 1
        self.updated_at = datetime.now(timezone.utc)

    def add_result(self, result: DerivativeResult) -> None:
        self.results.append(result)
        self.updated_at = datetime.now(timezone.utc)


class SourceAsset(BaseModel):
    """Metadata about a source media asset."""

    path: str
    content_hash: str
    media_type: MediaType
    tenant_id: str
    size_bytes: Optional[int] = None
    duration_seconds: Optional[float] = None
    width: Optional[int] = None
    height: Optional[int] = None
    codec: Optional[str] = None
    sample_rate: Optional[int] = None
    channels: Optional[int] = None
