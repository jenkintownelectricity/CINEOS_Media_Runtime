/**
 * Render Orchestrator Engine
 *
 * Manages render job lifecycle: create → start → complete/fail → retry.
 * All state changes emit render_event via the Canonical Event Spine.
 * Fail-closed: invalid transitions throw. No silent state changes.
 *
 * Classification: foundational render orchestration — not production render farm.
 * Persistence: localStorage-backed.
 */

export type RenderJobStatus = 'queued' | 'rendering' | 'completed' | 'failed' | 'cancelled';

export interface OutputFormat {
  resolution: string;
  codec: string;
  container: string;
  frame_rate: number;
}

export interface RenderJob {
  render_job_id: string;
  timeline_id: string;
  timeline_version_id: string;
  output_format: OutputFormat;
  status: RenderJobStatus;
  progress_pct: number;
  started_at: string | null;
  completed_at: string | null;
  output_ref: string | null;
  error_detail: string | null;
  retry_count: number;
  max_retries: number;
}

export interface EventEnvelope {
  event_id: string;
  event_class: string;
  source_subsystem: string;
  source_object_id: string;
  related_cdg_object_ids: string[];
  payload: Record<string, unknown>;
  status: string;
  emitted_at: string;
  actor_ref: string;
  correlation_id: string;
  causality_ref: string | null;
  replayable_flag: boolean;
}

export interface EventEmitResult {
  success: boolean;
  event_id?: string;
  error?: string;
}

export interface EventEmitterPort {
  emit(event: EventEnvelope): EventEmitResult;
}

function generateId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

const STORAGE_KEY = 'cineos:render-orchestrator:jobs';

/**
 * Valid state transitions for render jobs.
 * Fail-closed: any transition not listed here throws.
 */
const VALID_TRANSITIONS: Record<RenderJobStatus, RenderJobStatus[]> = {
  queued: ['rendering', 'cancelled'],
  rendering: ['completed', 'failed', 'cancelled'],
  completed: [],
  failed: ['queued'],   // retry requeues
  cancelled: [],
};

export class RenderOrchestratorEngine {
  private jobs: Map<string, RenderJob> = new Map();
  private emitter: EventEmitterPort;
  private lastEventIds: string[] = [];

  constructor(emitter: EventEmitterPort) {
    this.emitter = emitter;
    this.loadFromStorage();
  }

  private loadFromStorage(): void {
    try {
      if (typeof localStorage !== 'undefined') {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (raw) {
          const arr: RenderJob[] = JSON.parse(raw);
          for (const job of arr) {
            this.jobs.set(job.render_job_id, job);
          }
        }
      }
    } catch {
      // Storage unavailable — operate in-memory
    }
  }

  private persistToStorage(): void {
    try {
      if (typeof localStorage !== 'undefined') {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(Array.from(this.jobs.values())));
      }
    } catch (e) {
      throw new Error(`Render job persistence failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  private emitRenderEvent(
    job: RenderJob,
    action: string,
    causality: string | null = null,
    extraPayload: Record<string, unknown> = {},
  ): string {
    const eventId = generateId();
    const event: EventEnvelope = {
      event_id: eventId,
      event_class: 'render_event',
      source_subsystem: 'render_pipeline',
      source_object_id: job.render_job_id,
      related_cdg_object_ids: [job.timeline_id, job.timeline_version_id],
      payload: {
        action,
        render_job_id: job.render_job_id,
        status: job.status,
        progress_pct: job.progress_pct,
        ...extraPayload,
      },
      status: 'emitted',
      emitted_at: new Date().toISOString(),
      actor_ref: 'system:render_orchestrator',
      correlation_id: job.render_job_id,
      causality_ref: causality,
      replayable_flag: true,
    };

    const result = this.emitter.emit(event);
    if (!result.success) {
      throw new Error(`Failed to emit render_event: ${result.error}`);
    }
    this.lastEventIds.push(eventId);
    return eventId;
  }

  private validateTransition(current: RenderJobStatus, target: RenderJobStatus): void {
    if (!VALID_TRANSITIONS[current].includes(target)) {
      throw new Error(
        `Invalid render job transition: ${current} → ${target}. ` +
        `Allowed from ${current}: [${VALID_TRANSITIONS[current].join(', ')}]`
      );
    }
  }

  /**
   * Create a new render job in queued state.
   */
  createRenderJob(
    timelineId: string,
    versionId: string,
    outputFormat: OutputFormat,
    maxRetries: number = 3,
  ): RenderJob {
    if (!timelineId) throw new Error('timelineId is required');
    if (!versionId) throw new Error('versionId is required');
    if (!outputFormat) throw new Error('outputFormat is required');

    const job: RenderJob = {
      render_job_id: generateId(),
      timeline_id: timelineId,
      timeline_version_id: versionId,
      output_format: outputFormat,
      status: 'queued',
      progress_pct: 0,
      started_at: null,
      completed_at: null,
      output_ref: null,
      error_detail: null,
      retry_count: 0,
      max_retries: maxRetries,
    };

    this.jobs.set(job.render_job_id, job);
    this.persistToStorage();
    this.emitRenderEvent(job, 'created');
    return { ...job };
  }

  /**
   * Start rendering. Transitions queued → rendering.
   */
  startRender(renderJobId: string): RenderJob {
    const job = this.jobs.get(renderJobId);
    if (!job) throw new Error(`Render job not found: ${renderJobId}`);

    this.validateTransition(job.status, 'rendering');
    job.status = 'rendering';
    job.started_at = new Date().toISOString();
    job.progress_pct = 0;

    this.persistToStorage();
    this.emitRenderEvent(job, 'started');
    return { ...job };
  }

  /**
   * Complete rendering. Transitions rendering → completed.
   */
  completeRender(renderJobId: string, outputRef: string): RenderJob {
    const job = this.jobs.get(renderJobId);
    if (!job) throw new Error(`Render job not found: ${renderJobId}`);
    if (!outputRef) throw new Error('outputRef is required');

    this.validateTransition(job.status, 'completed');
    job.status = 'completed';
    job.progress_pct = 100;
    job.completed_at = new Date().toISOString();
    job.output_ref = outputRef;

    this.persistToStorage();
    this.emitRenderEvent(job, 'completed', null, { output_ref: outputRef });
    return { ...job };
  }

  /**
   * Fail rendering. Transitions rendering → failed.
   */
  failRender(renderJobId: string, error: string): RenderJob {
    const job = this.jobs.get(renderJobId);
    if (!job) throw new Error(`Render job not found: ${renderJobId}`);
    if (!error) throw new Error('error detail is required');

    this.validateTransition(job.status, 'failed');
    job.status = 'failed';
    job.error_detail = error;
    job.completed_at = new Date().toISOString();

    const canRetry = job.retry_count < job.max_retries;
    this.persistToStorage();
    this.emitRenderEvent(job, 'failed', null, {
      error_detail: error,
      can_retry: canRetry,
      retry_count: job.retry_count,
      max_retries: job.max_retries,
    });
    return { ...job };
  }

  /**
   * Retry a failed render. Increments retry_count and requeues if under max.
   */
  retryRender(renderJobId: string): RenderJob {
    const job = this.jobs.get(renderJobId);
    if (!job) throw new Error(`Render job not found: ${renderJobId}`);

    if (job.status !== 'failed') {
      throw new Error(`Cannot retry render job in status: ${job.status}. Must be 'failed'.`);
    }
    if (job.retry_count >= job.max_retries) {
      throw new Error(
        `Render job ${renderJobId} has exhausted retries (${job.retry_count}/${job.max_retries}).`
      );
    }

    job.retry_count += 1;
    job.status = 'queued';
    job.error_detail = null;
    job.started_at = null;
    job.completed_at = null;
    job.progress_pct = 0;

    this.persistToStorage();
    this.emitRenderEvent(job, 'retried', null, { retry_count: job.retry_count });
    return { ...job };
  }

  /**
   * Get current render job state.
   */
  getRenderJob(renderJobId: string): RenderJob | undefined {
    const job = this.jobs.get(renderJobId);
    return job ? { ...job } : undefined;
  }

  /**
   * Get all render jobs for a timeline.
   */
  getJobsForTimeline(timelineId: string): RenderJob[] {
    return Array.from(this.jobs.values())
      .filter(j => j.timeline_id === timelineId)
      .map(j => ({ ...j }));
  }

  /**
   * Get last emitted event IDs (for testing/inspection).
   */
  getLastEventIds(): string[] {
    return [...this.lastEventIds];
  }
}
