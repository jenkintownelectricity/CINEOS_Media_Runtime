/**
 * Media Cache State Contract
 *
 * Exposes the proxy / original / cache state for content-addressed
 * media assets managed by the runtime.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export enum CacheStatus {
  /** The asset is cached and available for immediate access. */
  CACHED = "CACHED",

  /** The asset was previously cached but has been evicted. */
  EVICTED = "EVICTED",

  /** The asset is being fetched / generated and is not yet available. */
  PENDING = "PENDING",

  /** An error occurred while caching the asset. */
  ERROR = "ERROR",
}

export interface MediaCacheState {
  /** Whether the original full-resolution asset is available locally. */
  hasOriginal: boolean;

  /** Whether a proxy (lower-resolution) version is available. */
  hasProxy: boolean;

  /** Resolution string of the proxy, if available (e.g. "960x540"). */
  proxyResolution?: string;

  /** Current cache status of the asset. */
  cacheStatus: CacheStatus;

  /** ISO-8601 timestamp of the last access to this asset. */
  lastAccessed?: string;
}

// ---------------------------------------------------------------------------
// Contract
// ---------------------------------------------------------------------------

export interface MediaCacheStateContract {
  /**
   * Retrieve the cache state for a media asset identified by its content hash.
   *
   * @param contentHash - Content-addressed hash of the asset.
   * @returns The current cache/proxy state.
   */
  getMediaCacheState(contentHash: string): Promise<MediaCacheState>;
}
