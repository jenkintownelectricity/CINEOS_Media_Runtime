/**
 * Content-Addressed Media Identity Contract
 *
 * Declares the canonical media identity strategy for CINEOS.
 * Canonical identity: SHA-256 content hash of source file.
 * Derived identities: asset_id (application-level), media_id (runtime-level).
 */

export interface ContentIdentity {
  /** SHA-256 hash of the source media file content. Canonical identity. */
  content_hash: string;
  /** Application-level asset identifier. Derived from content_hash. */
  asset_id: string;
  /** Runtime-level media identifier. Equal to content_hash in CINEOS_Media_Runtime. */
  media_id: string;
  /** Identity strategy: always 'sha256-content-addressed' for this system. */
  identity_strategy: 'sha256-content-addressed';
}

export interface ContentIdentityService {
  /** Look up a media identity by content hash. */
  getIdentity(contentHash: string): Promise<ContentIdentity | null>;
  /** Verify that a content hash matches the canonical identity of an asset. */
  verifyIdentity(assetId: string, expectedHash: string): Promise<boolean>;
}
