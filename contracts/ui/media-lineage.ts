/**
 * Media Lineage Visibility Contract
 *
 * Exposes derivation chains between media assets.
 * Mirrors transcode/provenance.py W3C PROV model.
 *
 * Lineage uses stable content-addressed identity (SHA-256 content hashes).
 */

export type LineageLinkType =
  | 'source_to_proxy'
  | 'source_to_cache'
  | 'source_to_render_output'
  | 'source_to_thumbnail'
  | 'source_to_waveform'
  | 'source_to_contact_sheet';

export interface LineageLink {
  link_id: string;
  source_content_hash: string;
  derived_content_hash: string;
  link_type: LineageLinkType;
  transform_id: string;
  created_at: string;
  agent: string;
}

export interface LineageChain {
  root_content_hash: string;
  root_filename: string;
  derivatives: LineageLink[];
  total_derivatives: number;
  chain_complete: boolean;
  missing_links: string[];
}

export interface MediaLineageService {
  getLineageChain(contentHash: string): Promise<LineageChain | null>;
  getDerivatives(contentHash: string): Promise<LineageLink[]>;
  getReverseLineage(derivedHash: string): Promise<LineageLink | null>;
}
