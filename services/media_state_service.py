"""
Media State Service — CINEOS Media Runtime
Agent 07-08 (Lane C — Media State Visibility)

In-memory, thread-safe service that aggregates media state.
Media runtime is SCAFFOLD — this service is honest about partial knowledge.
"""

from __future__ import annotations

import threading
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from ..contracts.media_state import (
    MediaAssetState,
    MediaStateSnapshot,
    MediaStateSummary,
    INGEST_STATUSES,
    PROXY_STATUSES,
    CACHE_STATUSES,
)


class MediaStateService:
    """
    Aggregates and manages media asset state.

    Thread-safe via threading.Lock. All state is in-memory.
    Classifies honestly: if partial data, says partial.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._assets: dict[str, MediaAssetState] = {}

    # -------------------------------------------------------------------
    # Queries
    # -------------------------------------------------------------------

    def get_snapshot(self) -> MediaStateSnapshot:
        """Return a point-in-time snapshot of all media state."""
        with self._lock:
            assets = list(self._assets.values())
            now = datetime.now(timezone.utc).isoformat()

            ingested = sum(1 for a in assets if a.ingest_status == "ingested")
            proxy_available = sum(1 for a in assets if a.proxy_status == "available")
            failed = sum(
                1 for a in assets
                if a.ingest_status == "failed" or a.proxy_status == "failed"
            )
            unknown = sum(
                1 for a in assets
                if a.ingest_status == "unknown" and a.proxy_status == "unknown"
            )

            return MediaStateSnapshot(
                snapshot_id=str(uuid.uuid4()),
                timestamp=now,
                assets=assets,
                total_assets=len(assets),
                ingested_count=ingested,
                proxy_available_count=proxy_available,
                failed_count=failed,
                unknown_count=unknown,
            )

    def get_summary(self) -> MediaStateSummary:
        """Return an aggregated summary of media state."""
        with self._lock:
            assets = list(self._assets.values())

            by_ingest: dict[str, int] = defaultdict(int)
            by_proxy: dict[str, int] = defaultdict(int)
            by_type: dict[str, int] = defaultdict(int)

            for a in assets:
                by_ingest[a.ingest_status] += 1
                by_proxy[a.proxy_status] += 1
                by_type[a.media_type] += 1

            return MediaStateSummary(
                total=len(assets),
                by_ingest_status=dict(by_ingest),
                by_proxy_status=dict(by_proxy),
                by_media_type=dict(by_type),
            )

    def get_asset_state(self, asset_id: str) -> Optional[MediaAssetState]:
        """Return the state of a single asset, or None if not found."""
        with self._lock:
            return self._assets.get(asset_id)

    # -------------------------------------------------------------------
    # Mutations
    # -------------------------------------------------------------------

    def register_asset(
        self,
        asset_id: str,
        filename: str,
        media_type: str,
        original_path: str,
    ) -> MediaAssetState:
        """
        Register a new media asset with unknown ingest/proxy/cache state.
        Honest default: everything starts as unknown until proven otherwise.
        """
        now = datetime.now(timezone.utc).isoformat()
        asset = MediaAssetState(
            asset_id=asset_id,
            filename=filename,
            media_type=media_type,
            ingest_status="unknown",
            proxy_status="unknown",
            cache_status="unknown",
            original_path=original_path,
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._assets[asset_id] = asset
        return asset

    def update_ingest_status(self, asset_id: str, status: str) -> MediaAssetState:
        """Update the ingest status of an asset."""
        if status not in INGEST_STATUSES:
            raise ValueError(f"Invalid ingest_status: {status!r}")
        with self._lock:
            asset = self._assets.get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")
            asset.ingest_status = status
            asset.updated_at = datetime.now(timezone.utc).isoformat()
            return asset

    def update_proxy_status(
        self,
        asset_id: str,
        status: str,
        proxy_path: Optional[str] = None,
    ) -> MediaAssetState:
        """Update the proxy status of an asset, optionally setting proxy_path."""
        if status not in PROXY_STATUSES:
            raise ValueError(f"Invalid proxy_status: {status!r}")
        with self._lock:
            asset = self._assets.get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")
            asset.proxy_status = status
            if proxy_path is not None:
                asset.proxy_path = proxy_path
            asset.updated_at = datetime.now(timezone.utc).isoformat()
            return asset

    def update_cache_status(self, asset_id: str, status: str) -> MediaAssetState:
        """Update the cache status of an asset."""
        if status not in CACHE_STATUSES:
            raise ValueError(f"Invalid cache_status: {status!r}")
        with self._lock:
            asset = self._assets.get(asset_id)
            if asset is None:
                raise KeyError(f"Asset not found: {asset_id}")
            asset.cache_status = status
            asset.updated_at = datetime.now(timezone.utc).isoformat()
            return asset
