"""
Media State Contract — CINEOS Media Runtime
Agent 07-08 (Lane C — Media State Visibility)

Data classes exposing honest media state visibility.
Media runtime is SCAFFOLD — states reflect partial knowledge truthfully.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional


# ---------------------------------------------------------------------------
# Enumerations (as string literals for serialization simplicity)
# ---------------------------------------------------------------------------

MEDIA_TYPES = ("video", "audio", "image", "sequence")

INGEST_STATUSES = ("pending", "ingesting", "ingested", "failed", "unknown")

PROXY_STATUSES = ("none", "generating", "available", "failed", "unknown")

CACHE_STATUSES = ("cold", "warm", "hot", "evicted", "unknown")

COMPLETENESS_LEVELS = ("complete", "partial", "unknown")


# ---------------------------------------------------------------------------
# MediaAssetState
# ---------------------------------------------------------------------------

@dataclass
class MediaAssetState:
    """State of a single media asset in the runtime."""

    asset_id: str
    filename: str
    media_type: str  # video | audio | image | sequence
    ingest_status: str  # pending | ingesting | ingested | failed | unknown
    proxy_status: str  # none | generating | available | failed | unknown
    cache_status: str  # cold | warm | hot | evicted | unknown
    original_path: str
    proxy_path: Optional[str] = None
    content_hash: Optional[str] = None
    file_size_bytes: Optional[int] = None
    codec: Optional[str] = None
    duration_seconds: Optional[float] = None
    dimensions: Optional[tuple[int, int]] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def __post_init__(self) -> None:
        if self.media_type not in MEDIA_TYPES:
            raise ValueError(f"Invalid media_type: {self.media_type!r}. Must be one of {MEDIA_TYPES}")
        if self.ingest_status not in INGEST_STATUSES:
            raise ValueError(f"Invalid ingest_status: {self.ingest_status!r}. Must be one of {INGEST_STATUSES}")
        if self.proxy_status not in PROXY_STATUSES:
            raise ValueError(f"Invalid proxy_status: {self.proxy_status!r}. Must be one of {PROXY_STATUSES}")
        if self.cache_status not in CACHE_STATUSES:
            raise ValueError(f"Invalid cache_status: {self.cache_status!r}. Must be one of {CACHE_STATUSES}")

    def to_dict(self) -> dict:
        d = asdict(self)
        # Convert dimensions tuple to list for JSON serialization
        if d["dimensions"] is not None:
            d["dimensions"] = list(d["dimensions"])
        return d

    @classmethod
    def from_dict(cls, data: dict) -> MediaAssetState:
        d = dict(data)
        # Convert dimensions list back to tuple
        if d.get("dimensions") is not None:
            d["dimensions"] = tuple(d["dimensions"])
        return cls(**d)


# ---------------------------------------------------------------------------
# MediaStateSnapshot
# ---------------------------------------------------------------------------

@dataclass
class MediaStateSnapshot:
    """Point-in-time snapshot of all media asset states."""

    snapshot_id: str
    timestamp: str
    assets: list[MediaAssetState]
    total_assets: int
    ingested_count: int
    proxy_available_count: int
    failed_count: int
    unknown_count: int

    def to_dict(self) -> dict:
        return {
            "snapshot_id": self.snapshot_id,
            "timestamp": self.timestamp,
            "assets": [a.to_dict() for a in self.assets],
            "total_assets": self.total_assets,
            "ingested_count": self.ingested_count,
            "proxy_available_count": self.proxy_available_count,
            "failed_count": self.failed_count,
            "unknown_count": self.unknown_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> MediaStateSnapshot:
        return cls(
            snapshot_id=data["snapshot_id"],
            timestamp=data["timestamp"],
            assets=[MediaAssetState.from_dict(a) for a in data["assets"]],
            total_assets=data["total_assets"],
            ingested_count=data["ingested_count"],
            proxy_available_count=data["proxy_available_count"],
            failed_count=data["failed_count"],
            unknown_count=data["unknown_count"],
        )

    def classify_completeness(self) -> str:
        """
        Classify how complete our knowledge of media state is.

        Returns "complete" if all assets have known (non-unknown) states,
        "partial" if some are known and some are unknown,
        "unknown" if we have no assets or all are unknown.
        """
        if self.total_assets == 0:
            return "unknown"

        known_ingest = sum(
            1 for a in self.assets if a.ingest_status != "unknown"
        )
        known_proxy = sum(
            1 for a in self.assets if a.proxy_status != "unknown"
        )
        known_cache = sum(
            1 for a in self.assets if a.cache_status != "unknown"
        )

        total_fields = self.total_assets * 3
        known_fields = known_ingest + known_proxy + known_cache

        if known_fields == 0:
            return "unknown"
        if known_fields == total_fields:
            return "complete"
        return "partial"


# ---------------------------------------------------------------------------
# MediaStateSummary
# ---------------------------------------------------------------------------

@dataclass
class MediaStateSummary:
    """Aggregated summary of media state across all assets."""

    total: int
    by_ingest_status: dict[str, int]
    by_proxy_status: dict[str, int]
    by_media_type: dict[str, int]

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "by_ingest_status": dict(self.by_ingest_status),
            "by_proxy_status": dict(self.by_proxy_status),
            "by_media_type": dict(self.by_media_type),
        }

    @classmethod
    def from_dict(cls, data: dict) -> MediaStateSummary:
        return cls(
            total=data["total"],
            by_ingest_status=dict(data["by_ingest_status"]),
            by_proxy_status=dict(data["by_proxy_status"]),
            by_media_type=dict(data["by_media_type"]),
        )
