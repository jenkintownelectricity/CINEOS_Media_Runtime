# Media Panel Contract

> **Upstream repo:** CINEOS_Media_Runtime
> **Status:** Prototype-backed (mock data adapters, no real media pipeline yet)
> **Panel-runtime aware:** Yes — all data contracts are designed for consumption by registered CINEOS panels via typed adapter interfaces.
> **Consuming panels:** Media Browser Panel, Asset Detail Panel, Ingest Queue Panel

---

## 1. Overview

The Media Panel Contract defines the data shapes, adapter interfaces, and commands that CINEOS Media_Runtime exposes for media-centric panels running inside the CINEOS workspace host. Three panels consume this contract: the Media Browser Panel (browse, filter, and select assets), the Asset Detail Panel (full metadata and preview), and the Ingest Queue Panel (monitor ingest progress). All panels receive data through typed adapters and never access Media_Runtime internals directly.

---

## 2. Core Types

### 2.1 Asset

A media asset managed by the Media_Runtime.

```typescript
interface Asset {
  asset_id: string;                 // UUID
  name: string;                     // Display name
  file_name: string;                // Original file name
  type: AssetType;
  mime_type: string;                // e.g. "video/mp4", "image/png"
  file_size_bytes: number;
  duration_ms?: number;             // For video/audio assets
  dimensions?: AssetDimensions;     // For image/video assets
  sample_rate?: number;             // For audio assets (Hz)
  channels?: number;                // For audio assets
  codec?: string;                   // Primary codec
  color_space?: string;             // e.g. "sRGB", "Rec.709"
  frame_rate?: number;              // For video assets (fps)
  status: AssetStatus;
  created_at: string;               // ISO 8601
  updated_at: string;               // ISO 8601
  imported_at: string;              // ISO 8601
  tags: string[];
  collections: string[];            // Collection IDs this asset belongs to
  preview_urls: AssetPreviewUrls;
  metadata: Record<string, unknown>; // Extended / custom metadata
}

type AssetType =
  | "video"
  | "audio"
  | "image"
  | "subtitle"
  | "document"
  | "project_file"
  | "lut"                           // Color lookup table
  | "font"
  | "other";

type AssetStatus =
  | "available"                     // Ready for use
  | "ingesting"                     // Currently being ingested
  | "processing"                    // Post-ingest processing (transcoding, thumbnail generation)
  | "error"                         // Ingest or processing failed
  | "offline"                       // Source file not accessible
  | "archived";                     // Moved to cold storage

interface AssetDimensions {
  width: number;
  height: number;
}

interface AssetPreviewUrls {
  thumbnail_small?: string;         // ~100px, for grid views
  thumbnail_medium?: string;        // ~300px, for card views
  thumbnail_large?: string;         // ~800px, for detail views
  waveform?: string;                // Waveform image for audio assets
  proxy_video?: string;             // Low-res proxy for video playback
  full_resolution?: string;         // Full-res URL (may require auth)
}
```

---

## 3. Media Browser Data Contract

The Media Browser Panel provides a filterable, sortable, paginated view of assets.

### 3.1 Asset List Request

```typescript
interface AssetListRequest {
  query?: string;                   // Full-text search across name, tags, metadata
  filters?: AssetListFilters;
  sort_by?: AssetSortField;
  sort_order?: "asc" | "desc";
  view_mode?: "grid" | "list";      // Hint for adapter to optimize thumbnail loading
  limit?: number;                   // Default: 50, max: 200
  offset?: number;                  // Pagination offset
}

interface AssetListFilters {
  types?: AssetType[];
  statuses?: AssetStatus[];
  tags?: string[];                  // OR logic
  collections?: string[];           // OR logic
  mime_types?: string[];
  file_size_min?: number;           // bytes
  file_size_max?: number;           // bytes
  duration_min_ms?: number;
  duration_max_ms?: number;
  created_after?: string;           // ISO 8601
  created_before?: string;          // ISO 8601
  dimensions_min?: AssetDimensions;
  dimensions_max?: AssetDimensions;
}

type AssetSortField =
  | "name"
  | "created_at"
  | "updated_at"
  | "file_size_bytes"
  | "duration_ms"
  | "type";
```

### 3.2 Asset List Response

```typescript
interface AssetListResponse {
  assets: Asset[];
  total: number;
  limit: number;
  offset: number;
  facets: AssetFacets;              // For building filter UI
}

interface AssetFacets {
  types: FacetCount<AssetType>[];
  statuses: FacetCount<AssetStatus>[];
  tags: FacetCount<string>[];       // Top N tags by count
  collections: FacetCount<string>[];
}

interface FacetCount<T> {
  value: T;
  count: number;
}
```

---

## 4. Asset Detail Data Contract

The Asset Detail Panel displays full metadata, preview, and ingest status for a single asset.

### 4.1 AssetDetail

```typescript
interface AssetDetail {
  asset: Asset;
  technical_metadata: TechnicalMetadata;
  ingest_info: IngestInfo;
  usage: AssetUsage;
  history: AssetHistoryEntry[];
}

interface TechnicalMetadata {
  format_name: string;              // e.g. "QuickTime / MOV"
  format_long_name: string;
  bit_rate?: number;                // bps
  streams: StreamInfo[];
  container_metadata: Record<string, string>;
}

interface StreamInfo {
  stream_index: number;
  type: "video" | "audio" | "subtitle" | "data";
  codec_name: string;
  codec_long_name: string;
  bit_rate?: number;
  profile?: string;
  // Video-specific
  width?: number;
  height?: number;
  frame_rate?: number;
  pixel_format?: string;
  color_space?: string;
  // Audio-specific
  sample_rate?: number;
  channels?: number;
  channel_layout?: string;
}

interface IngestInfo {
  ingest_job_id?: string;           // Reference to the ingest job
  ingested_at?: string;             // ISO 8601
  source_path: string;              // Original file path or URI
  checksum_sha256?: string;
  ingest_duration_ms?: number;
  transcoding_status?: "pending" | "in_progress" | "complete" | "failed" | "skipped";
  proxy_generated: boolean;
  thumbnails_generated: boolean;
}

interface AssetUsage {
  used_in_timelines: AssetTimelineRef[];
  reference_count: number;
}

interface AssetTimelineRef {
  timeline_id: string;
  timeline_name: string;
  clip_ids: string[];
}

interface AssetHistoryEntry {
  timestamp: string;                // ISO 8601
  action: "imported" | "metadata_updated" | "relinked" | "archived" | "restored" | "deleted";
  description: string;
  actor: string;                    // User or system identifier
}
```

---

## 5. Ingest Queue Data Contract

The Ingest Queue Panel monitors active and recent ingest operations.

### 5.1 IngestQueue

```typescript
interface IngestQueue {
  active_jobs: IngestJob[];
  queued_jobs: IngestJob[];
  recent_jobs: IngestJob[];         // Last N completed/failed jobs
  summary: IngestQueueSummary;
}

interface IngestQueueSummary {
  active_count: number;
  queued_count: number;
  completed_today: number;
  failed_today: number;
  total_bytes_ingested_today: number;
  average_ingest_speed_bps: number;
}
```

### 5.2 IngestJob

```typescript
interface IngestJob {
  job_id: string;                   // UUID
  source_path: string;              // Original file path or URI
  file_name: string;
  file_size_bytes: number;
  status: IngestJobStatus;
  progress: IngestProgress;
  created_at: string;               // ISO 8601
  started_at?: string;              // ISO 8601
  completed_at?: string;            // ISO 8601
  asset_id?: string;                // Set once the asset is created
  error?: IngestError;
  options: IngestOptions;
}

type IngestJobStatus =
  | "queued"
  | "ingesting"                     // Copying/uploading the file
  | "processing"                    // Transcoding, thumbnail generation
  | "complete"
  | "failed"
  | "cancelled";

interface IngestProgress {
  phase: "upload" | "transcode" | "thumbnail" | "metadata" | "complete";
  phase_progress: number;           // 0.0 – 1.0 for the current phase
  overall_progress: number;         // 0.0 – 1.0 across all phases
  bytes_transferred: number;
  estimated_remaining_ms?: number;
  current_speed_bps?: number;       // bytes per second
}

interface IngestError {
  code: string;                     // Machine-readable error code
  message: string;                  // Human-readable description
  phase: string;                    // Which phase failed
  is_retryable: boolean;
  retry_count: number;
}

interface IngestOptions {
  generate_proxy: boolean;
  generate_thumbnails: boolean;
  auto_tag: boolean;                // Use AI to auto-tag the asset
  target_collection?: string;       // Collection to add the asset to
  metadata_overrides?: Record<string, unknown>;
}
```

---

## 6. Commands

Commands are dispatched through the workspace host command bus.

| Command                 | Payload                                        | Target Panel        | Description                                       |
|-------------------------|------------------------------------------------|---------------------|---------------------------------------------------|
| `open_in_media_browser` | `{ query?: string, filters?: AssetListFilters }` | Media Browser Panel | Open the Media Browser, optionally pre-filtered.  |
| `open_object`           | `{ object_type: "asset", object_id: string }`  | Asset Detail Panel  | Open the Asset Detail Panel for the specified asset. |

**Command dispatch via adapter:**

```typescript
interface MediaCommandAdapter {
  openInMediaBrowser(options?: { query?: string; filters?: AssetListFilters }): void;
  openObject(object_type: "asset", object_id: string): void;
}
```

---

## 7. Typed Adapter Interface

All data access goes through the adapter provided to panels at mount time.

```typescript
interface MediaPanelAdapter {
  // Media Browser
  listAssets(request: AssetListRequest): Promise<AssetListResponse>;
  getCollections(): Promise<Collection[]>;

  // Asset Detail
  getAssetDetail(asset_id: string): Promise<AssetDetail>;

  // Ingest Queue
  getIngestQueue(): Promise<IngestQueue>;
  getIngestJob(job_id: string): Promise<IngestJob>;
  cancelIngestJob(job_id: string): Promise<void>;
  retryIngestJob(job_id: string): Promise<IngestJob>;

  // Subscriptions
  subscribeToAssetUpdates(asset_id: string, callback: (asset: Asset) => void): Unsubscribe;
  subscribeToIngestQueue(callback: (queue: IngestQueue) => void): Unsubscribe;
  subscribeToIngestJob(job_id: string, callback: (job: IngestJob) => void): Unsubscribe;

  // Commands
  commands: MediaCommandAdapter;
}

interface Collection {
  collection_id: string;
  name: string;
  asset_count: number;
  created_at: string;
}

type Unsubscribe = () => void;
```

---

## 8. Prototype Implementation Notes

- `listAssets` returns a mock library of 25 assets spanning all asset types (video, audio, image, subtitle, LUT).
- Facets are pre-computed from the mock dataset and return accurate counts.
- `getAssetDetail` returns full mock metadata including synthetic stream info, ingest info, and usage references.
- `getIngestQueue` returns a mock queue with 2 active jobs, 3 queued jobs, and 5 recent completed/failed jobs.
- Ingest progress updates are static — `subscribeToIngestJob` fires once and does not simulate progress.
- Preview URLs in the mock data point to placeholder image/video endpoints.
- All mock data is deterministic and keyed on IDs for test stability.
