/**
 * Ingest Queue State Contract
 *
 * Provides visibility into the media ingest pipeline -- pending,
 * active, recently completed, and failed jobs.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type IngestJobStatus =
  | "pending"
  | "active"
  | "completed"
  | "failed";

export interface IngestJob {
  /** Unique job identifier. */
  id: string;

  /** Original filename being ingested. */
  filename: string;

  /** Content-addressed hash of the file. */
  contentHash: string;

  /** Current status of the job. */
  status: IngestJobStatus;

  /** Progress percentage (0-100). Only meaningful while active. */
  progress: number;

  /** ISO-8601 timestamp of when the job started processing. */
  startedAt?: string;
}

export interface IngestQueueStatus {
  /** Number of jobs waiting to be processed. */
  pendingCount: number;

  /** Jobs currently being processed. */
  activeJobs: IngestJob[];

  /** Recently completed jobs. */
  completedRecent: IngestJob[];

  /** Recently failed jobs. */
  failedRecent: IngestJob[];
}

// ---------------------------------------------------------------------------
// Contract
// ---------------------------------------------------------------------------

export interface IngestQueueState {
  /**
   * Retrieve a snapshot of the current ingest queue status.
   */
  getIngestQueueStatus(): Promise<IngestQueueStatus>;
}
