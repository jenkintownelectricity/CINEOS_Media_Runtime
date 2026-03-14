"""
CINEOS Media Runtime — Transcode CloudEvents

CloudEvents v1.0 compliant event emission for media derivative lifecycle.
All events are tenant-scoped and carry deterministic derivative identifiers.

Event types:
    media.transcode.started
    media.transcode.completed
    media.transcode.failed
    media.proxy.generated
    media.thumbnail.generated

Ref: https://cloudevents.io/
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# CloudEvent envelope
# ---------------------------------------------------------------------------


class CloudEvent(BaseModel):
    """CloudEvents v1.0 structured-mode envelope."""

    specversion: str = "1.0"
    id: str = Field(default_factory=lambda: uuid4().hex)
    source: str = Field(
        default="cineos://media-runtime/transcode",
        description="URI identifying the event source",
    )
    type: str = Field(..., description="Event type (e.g. media.transcode.completed)")
    subject: Optional[str] = Field(
        None, description="Subject of the event (e.g. derivative filename)"
    )
    time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    datacontenttype: str = "application/json"
    data: dict[str, Any] = Field(default_factory=dict)

    # CINEOS extensions
    tenantid: str = Field(..., description="Tenant scope")
    traceparent: Optional[str] = Field(
        None, description="W3C Trace Context traceparent"
    )

    def to_json(self) -> str:
        return self.model_dump_json(exclude_none=True)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(exclude_none=True)


# ---------------------------------------------------------------------------
# Event type constants
# ---------------------------------------------------------------------------

TRANSCODE_STARTED = "media.transcode.started"
TRANSCODE_COMPLETED = "media.transcode.completed"
TRANSCODE_FAILED = "media.transcode.failed"
PROXY_GENERATED = "media.proxy.generated"
THUMBNAIL_GENERATED = "media.thumbnail.generated"


# ---------------------------------------------------------------------------
# Event emitter
# ---------------------------------------------------------------------------

EventHandler = Callable[[CloudEvent], None]


class TranscodeEventEmitter:
    """
    Emits CloudEvents for transcode lifecycle transitions.
    Handlers can be registered per event type or as catch-all listeners.
    """

    def __init__(self, source: str = "cineos://media-runtime/transcode") -> None:
        self._source = source
        self._handlers: dict[str, list[EventHandler]] = {}
        self._global_handlers: list[EventHandler] = []
        self._event_log: list[CloudEvent] = []

    def on(self, event_type: str, handler: EventHandler) -> None:
        """Register a handler for a specific event type."""
        self._handlers.setdefault(event_type, []).append(handler)

    def on_all(self, handler: EventHandler) -> None:
        """Register a catch-all handler invoked for every event."""
        self._global_handlers.append(handler)

    def emit(self, event: CloudEvent) -> None:
        """Dispatch an event to registered handlers."""
        self._event_log.append(event)
        for handler in self._handlers.get(event.type, []):
            handler(event)
        for handler in self._global_handlers:
            handler(event)

    @property
    def event_log(self) -> list[CloudEvent]:
        """In-memory log of all emitted events (useful for testing)."""
        return list(self._event_log)

    def clear_log(self) -> None:
        self._event_log.clear()

    # --- Convenience emitters -------------------------------------------------

    def transcode_started(
        self,
        tenant_id: str,
        job_id: str,
        source_hash: str,
        source_path: str,
        profile_id: str,
        **extra: Any,
    ) -> CloudEvent:
        event = CloudEvent(
            type=TRANSCODE_STARTED,
            source=self._source,
            subject=f"{source_hash}_{profile_id}",
            tenantid=tenant_id,
            data={
                "job_id": job_id,
                "source_hash": source_hash,
                "source_path": source_path,
                "profile_id": profile_id,
                **extra,
            },
        )
        self.emit(event)
        return event

    def transcode_completed(
        self,
        tenant_id: str,
        job_id: str,
        source_hash: str,
        profile_id: str,
        output_path: str,
        output_hash: str,
        duration_seconds: float,
        output_size_bytes: int,
        **extra: Any,
    ) -> CloudEvent:
        event = CloudEvent(
            type=TRANSCODE_COMPLETED,
            source=self._source,
            subject=f"{source_hash}_{profile_id}",
            tenantid=tenant_id,
            data={
                "job_id": job_id,
                "source_hash": source_hash,
                "profile_id": profile_id,
                "output_path": output_path,
                "output_hash": output_hash,
                "duration_seconds": duration_seconds,
                "output_size_bytes": output_size_bytes,
                **extra,
            },
        )
        self.emit(event)
        return event

    def transcode_failed(
        self,
        tenant_id: str,
        job_id: str,
        source_hash: str,
        profile_id: str,
        error: str,
        retry_count: int = 0,
        **extra: Any,
    ) -> CloudEvent:
        event = CloudEvent(
            type=TRANSCODE_FAILED,
            source=self._source,
            subject=f"{source_hash}_{profile_id}",
            tenantid=tenant_id,
            data={
                "job_id": job_id,
                "source_hash": source_hash,
                "profile_id": profile_id,
                "error": error,
                "retry_count": retry_count,
                **extra,
            },
        )
        self.emit(event)
        return event

    def proxy_generated(
        self,
        tenant_id: str,
        source_hash: str,
        profile_id: str,
        output_path: str,
        output_hash: str,
        **extra: Any,
    ) -> CloudEvent:
        event = CloudEvent(
            type=PROXY_GENERATED,
            source=self._source,
            subject=f"{source_hash}_{profile_id}",
            tenantid=tenant_id,
            data={
                "source_hash": source_hash,
                "profile_id": profile_id,
                "output_path": output_path,
                "output_hash": output_hash,
                **extra,
            },
        )
        self.emit(event)
        return event

    def thumbnail_generated(
        self,
        tenant_id: str,
        source_hash: str,
        profile_id: str,
        output_path: str,
        output_hash: str,
        **extra: Any,
    ) -> CloudEvent:
        event = CloudEvent(
            type=THUMBNAIL_GENERATED,
            source=self._source,
            subject=f"{source_hash}_{profile_id}",
            tenantid=tenant_id,
            data={
                "source_hash": source_hash,
                "profile_id": profile_id,
                "output_path": output_path,
                "output_hash": output_hash,
                **extra,
            },
        )
        self.emit(event)
        return event
