/**
 * Render Orchestration Contracts
 *
 * Defines the render request, job status, output reference, failure,
 * and queue visibility models for CINEOS media pipeline.
 *
 * Status: CONTRACT-ONLY — no live render execution backing.
 * Mirrors transcode/models.py TranscodeJob and DerivativeResult.
 */

export type RenderJobStatus = 'queued' | 'rendering' | 'completed' | 'failed' | 'cancelled';

export interface RenderRequest {
  request_id: string;
  source_content_hash: string;
  source_filename: string;
  output_format: string;
  render_profile: string;
  parameters: Record<string, unknown>;
  requested_at: string;
  requested_by: string;
}

export interface RenderJob {
  job_id: string;
  request: RenderRequest;
  status: RenderJobStatus;
  progress_percent: number;
  started_at: string | null;
  completed_at: string | null;
  error_message: string | null;
  output_ref: RenderOutputRef | null;
  retry_count: number;
  max_retries: number;
}

export interface RenderOutputRef {
  output_content_hash: string;
  output_path: string;
  output_format: string;
  output_size_bytes: number;
  derived_from_hash: string;
}

export interface RenderFailure {
  job_id: string;
  error_code: string;
  error_message: string;
  failed_at: string;
  recoverable: boolean;
}

export interface RenderQueueStatus {
  queued_count: number;
  active_jobs: RenderJob[];
  completed_recent: RenderJob[];
  failed_recent: RenderFailure[];
  total_completed: number;
  total_failed: number;
}

export interface RenderOrchestrationService {
  submitRenderRequest(request: RenderRequest): Promise<RenderJob>;
  getRenderJob(jobId: string): Promise<RenderJob | null>;
  getQueueStatus(): Promise<RenderQueueStatus>;
  cancelRenderJob(jobId: string): Promise<boolean>;
}
