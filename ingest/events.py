"""
CINEOS Media Runtime — CloudEvents for media ingest.

All events follow the CloudEvents specification (v1.0) with:
  specversion, id, source, type, time, tenant_id, data

Event types:
  - media.upload.initiated
  - media.upload.completed
  - media.registered
  - media.ingest.completed
  - media.ingest.failed
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any, Optional, Protocol

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# CloudEvent envelope
# ---------------------------------------------------------------------------

EVENT_SOURCE = "cineos.media.ingest"
SPEC_VERSION = "1.0"


class CloudEvent(BaseModel):
    """CloudEvents v1.0 envelope."""
    specversion: str = Field(default=SPEC_VERSION, description="CloudEvents spec version")
    id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique event identifier",
    )
    source: str = Field(default=EVENT_SOURCE, description="Event source URI")
    type: str = Field(..., description="Event type identifier")
    time: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc),
        description="Event timestamp (ISO-8601)",
    )
    tenant_id: str = Field(..., description="Tenant scope for the event")
    subject: Optional[str] = Field(default=None, description="Event subject (e.g. media_id)")
    data: dict[str, Any] = Field(default_factory=dict, description="Event payload")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary suitable for JSON transport."""
        return self.model_dump(mode="json")


# ---------------------------------------------------------------------------
# Event type constants
# ---------------------------------------------------------------------------

class EventType:
    UPLOAD_INITIATED = "media.upload.initiated"
    UPLOAD_COMPLETED = "media.upload.completed"
    REGISTERED = "media.registered"
    INGEST_COMPLETED = "media.ingest.completed"
    INGEST_FAILED = "media.ingest.failed"


# ---------------------------------------------------------------------------
# Event emitter protocol and default implementation
# ---------------------------------------------------------------------------

class EventEmitter(Protocol):
    """Protocol for publishing CloudEvents."""

    async def emit(self, event: CloudEvent) -> None: ...


class InMemoryEventEmitter:
    """
    Simple in-memory event emitter for testing and local development.
    Stores emitted events in an ordered list.
    """

    def __init__(self) -> None:
        self.events: list[CloudEvent] = []

    async def emit(self, event: CloudEvent) -> None:
        self.events.append(event)

    def get_events(self, event_type: Optional[str] = None) -> list[CloudEvent]:
        if event_type is None:
            return list(self.events)
        return [e for e in self.events if e.type == event_type]

    def clear(self) -> None:
        self.events.clear()


# ---------------------------------------------------------------------------
# Event factory helpers
# ---------------------------------------------------------------------------

def _make_event(
    event_type: str,
    tenant_id: str,
    data: dict[str, Any],
    subject: Optional[str] = None,
) -> CloudEvent:
    return CloudEvent(
        type=event_type,
        tenant_id=tenant_id,
        subject=subject,
        data=data,
    )


def upload_initiated(
    tenant_id: str,
    upload_id: str,
    filename: str,
    content_type: str,
    size_bytes: int,
) -> CloudEvent:
    """Create a media.upload.initiated event."""
    return _make_event(
        event_type=EventType.UPLOAD_INITIATED,
        tenant_id=tenant_id,
        subject=upload_id,
        data={
            "upload_id": upload_id,
            "filename": filename,
            "content_type": content_type,
            "size_bytes": size_bytes,
        },
    )


def upload_completed(
    tenant_id: str,
    upload_id: str,
    storage_bucket: str,
    storage_key: str,
    size_bytes: int,
) -> CloudEvent:
    """Create a media.upload.completed event."""
    return _make_event(
        event_type=EventType.UPLOAD_COMPLETED,
        tenant_id=tenant_id,
        subject=upload_id,
        data={
            "upload_id": upload_id,
            "storage_bucket": storage_bucket,
            "storage_key": storage_key,
            "size_bytes": size_bytes,
        },
    )


def media_registered(
    tenant_id: str,
    media_id: str,
    filename: str,
    content_type: str,
    size_bytes: int,
    is_duplicate: bool,
) -> CloudEvent:
    """Create a media.registered event."""
    return _make_event(
        event_type=EventType.REGISTERED,
        tenant_id=tenant_id,
        subject=media_id,
        data={
            "media_id": media_id,
            "filename": filename,
            "content_type": content_type,
            "size_bytes": size_bytes,
            "is_duplicate": is_duplicate,
        },
    )


def ingest_completed(
    tenant_id: str,
    ingest_id: str,
    media_id: str,
    derivative_job_ids: list[str],
) -> CloudEvent:
    """Create a media.ingest.completed event."""
    return _make_event(
        event_type=EventType.INGEST_COMPLETED,
        tenant_id=tenant_id,
        subject=media_id,
        data={
            "ingest_id": ingest_id,
            "media_id": media_id,
            "derivative_job_ids": derivative_job_ids,
        },
    )


def ingest_failed(
    tenant_id: str,
    ingest_id: str,
    error: str,
    stage: str,
) -> CloudEvent:
    """Create a media.ingest.failed event."""
    return _make_event(
        event_type=EventType.INGEST_FAILED,
        tenant_id=tenant_id,
        subject=ingest_id,
        data={
            "ingest_id": ingest_id,
            "error": error,
            "stage": stage,
        },
    )
