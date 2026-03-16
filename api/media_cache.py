"""
CINEOS Media Runtime — Media Cache Manager

In-memory cache tracker with LRU eviction, TTL expiry, and memory
budget tracking.  Implements the MediaCacheState TypeScript contract.

Thread-safe via threading.Lock.  Pure stdlib — no external deps.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Cache entry
# ---------------------------------------------------------------------------

class CacheEntry:
    """A single cached media asset."""

    __slots__ = (
        "content_hash",
        "has_original",
        "has_proxy",
        "proxy_resolution",
        "size_bytes",
        "last_accessed",
        "created_at",
        "ttl_seconds",
        "status",
    )

    def __init__(
        self,
        content_hash: str,
        *,
        has_original: bool = True,
        has_proxy: bool = False,
        proxy_resolution: Optional[str] = None,
        size_bytes: int = 0,
        ttl_seconds: int = 3600,
    ) -> None:
        self.content_hash = content_hash
        self.has_original = has_original
        self.has_proxy = has_proxy
        self.proxy_resolution = proxy_resolution
        self.size_bytes = size_bytes
        now = datetime.now(timezone.utc).isoformat()
        self.last_accessed = now
        self.created_at = now
        self.ttl_seconds = ttl_seconds
        self.status: str = "CACHED"

    def touch(self) -> None:
        """Update last-accessed timestamp."""
        self.last_accessed = datetime.now(timezone.utc).isoformat()

    def is_expired(self) -> bool:
        """Check whether this entry has exceeded its TTL."""
        accessed = datetime.fromisoformat(self.last_accessed)
        elapsed = (datetime.now(timezone.utc) - accessed).total_seconds()
        return elapsed > self.ttl_seconds

    def to_media_cache_state(self) -> dict[str, Any]:
        """Serialize to the MediaCacheState TS contract shape."""
        result: dict[str, Any] = {
            "hasOriginal": self.has_original,
            "hasProxy": self.has_proxy,
            "cacheStatus": self.status,
            "lastAccessed": self.last_accessed,
        }
        if self.proxy_resolution is not None:
            result["proxyResolution"] = self.proxy_resolution
        return result


# ---------------------------------------------------------------------------
# MediaCache
# ---------------------------------------------------------------------------

class MediaCache:
    """
    In-memory LRU media cache with memory budget and TTL support.

    Parameters
    ----------
    max_memory_bytes
        Maximum total memory budget for cached entries.
        When exceeded, least-recently-used entries are evicted.
    default_ttl
        Default time-to-live in seconds for new entries.
    """

    def __init__(
        self,
        max_memory_bytes: int = 2 * 1024 * 1024 * 1024,  # 2 GiB
        default_ttl: int = 3600,
    ) -> None:
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, CacheEntry] = OrderedDict()
        self._evicted: dict[str, CacheEntry] = {}
        self.max_memory_bytes = max_memory_bytes
        self.default_ttl = default_ttl
        self._total_bytes: int = 0
        self._total_evictions: int = 0
        self._total_hits: int = 0
        self._total_misses: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def put(
        self,
        content_hash: str,
        *,
        has_original: bool = True,
        has_proxy: bool = False,
        proxy_resolution: Optional[str] = None,
        size_bytes: int = 0,
        ttl_seconds: Optional[int] = None,
    ) -> CacheEntry:
        """Add or update a cache entry. Evicts LRU entries if over budget."""
        ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl

        with self._lock:
            # Update existing entry
            if content_hash in self._entries:
                existing = self._entries[content_hash]
                self._total_bytes -= existing.size_bytes
                self._entries.move_to_end(content_hash)
                existing.has_original = has_original
                existing.has_proxy = has_proxy
                existing.proxy_resolution = proxy_resolution
                existing.size_bytes = size_bytes
                existing.status = "CACHED"
                existing.touch()
                self._total_bytes += size_bytes
                self._evict_if_needed()
                return existing

            entry = CacheEntry(
                content_hash=content_hash,
                has_original=has_original,
                has_proxy=has_proxy,
                proxy_resolution=proxy_resolution,
                size_bytes=size_bytes,
                ttl_seconds=ttl,
            )
            self._entries[content_hash] = entry
            self._total_bytes += size_bytes

            # Remove from evicted tracking if re-cached
            self._evicted.pop(content_hash, None)

            self._evict_if_needed()
            return entry

    def get(self, content_hash: str) -> Optional[CacheEntry]:
        """Retrieve a cache entry. Returns None if not found or expired."""
        with self._lock:
            entry = self._entries.get(content_hash)
            if entry is None:
                self._total_misses += 1
                return None

            if entry.is_expired():
                self._do_evict(content_hash)
                self._total_misses += 1
                return None

            entry.touch()
            self._entries.move_to_end(content_hash)
            self._total_hits += 1
            return entry

    def get_state(self, content_hash: str) -> dict[str, Any]:
        """
        Return the MediaCacheState contract for an asset.
        Covers cached, evicted, and unknown states.
        """
        with self._lock:
            entry = self._entries.get(content_hash)
            if entry is not None:
                if entry.is_expired():
                    self._do_evict(content_hash)
                else:
                    entry.touch()
                    self._entries.move_to_end(content_hash)
                    self._total_hits += 1
                    return entry.to_media_cache_state()

            # Check evicted
            evicted = self._evicted.get(content_hash)
            if evicted is not None:
                self._total_misses += 1
                return {
                    "hasOriginal": evicted.has_original,
                    "hasProxy": evicted.has_proxy,
                    "cacheStatus": "EVICTED",
                    "lastAccessed": evicted.last_accessed,
                }

            # Not known
            self._total_misses += 1
            return {
                "hasOriginal": False,
                "hasProxy": False,
                "cacheStatus": "PENDING",
            }

    def get_full_state(self) -> dict[str, Any]:
        """Return aggregated cache state for the /api/cache endpoint."""
        with self._lock:
            # Expire stale entries first
            expired_keys = [
                k for k, v in self._entries.items() if v.is_expired()
            ]
            for k in expired_keys:
                self._do_evict(k)

            entries = []
            for content_hash, entry in self._entries.items():
                item = entry.to_media_cache_state()
                item["contentHash"] = content_hash
                item["sizeBytes"] = entry.size_bytes
                entries.append(item)

            return {
                "entries": entries,
                "totalEntries": len(self._entries),
                "totalBytes": self._total_bytes,
                "maxBytes": self.max_memory_bytes,
                "utilizationPercent": round(
                    (self._total_bytes / self.max_memory_bytes * 100)
                    if self.max_memory_bytes > 0
                    else 0,
                    2,
                ),
                "totalEvictions": self._total_evictions,
                "totalHits": self._total_hits,
                "totalMisses": self._total_misses,
                "hitRate": round(
                    (
                        self._total_hits
                        / (self._total_hits + self._total_misses)
                        * 100
                    )
                    if (self._total_hits + self._total_misses) > 0
                    else 0,
                    2,
                ),
            }

    def remove(self, content_hash: str) -> bool:
        """Explicitly remove an entry from the cache."""
        with self._lock:
            if content_hash in self._entries:
                self._do_evict(content_hash)
                return True
            return False

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _evict_if_needed(self) -> None:
        """Evict least-recently-used entries until under memory budget."""
        while self._total_bytes > self.max_memory_bytes and self._entries:
            oldest_key, _ = next(iter(self._entries.items()))
            self._do_evict(oldest_key)

    def _do_evict(self, content_hash: str) -> None:
        """Evict a single entry by content_hash."""
        entry = self._entries.pop(content_hash, None)
        if entry is not None:
            self._total_bytes -= entry.size_bytes
            entry.status = "EVICTED"
            self._evicted[content_hash] = entry
            self._total_evictions += 1
