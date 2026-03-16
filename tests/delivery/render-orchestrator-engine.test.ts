/**
 * Render Orchestrator Engine — Tests
 *
 * 8 tests covering:
 * - Create/start/complete render lifecycle
 * - Fail and retry path
 * - Event emission per state change
 * - Fail-closed on invalid transitions
 * - Output reference persistence
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  RenderOrchestratorEngine,
  EventEmitterPort,
  EventEnvelope,
  EventEmitResult,
} from '../../src/delivery/render-orchestrator-engine';

// --- Mock Event Emitter ---
class MockEventEmitter implements EventEmitterPort {
  public events: EventEnvelope[] = [];

  emit(event: EventEnvelope): EventEmitResult {
    this.events.push(event);
    return { success: true, event_id: event.event_id };
  }
}

// --- Mock localStorage ---
const store: Record<string, string> = {};
(globalThis as any).localStorage = {
  getItem: (key: string) => store[key] ?? null,
  setItem: (key: string, val: string) => { store[key] = val; },
  removeItem: (key: string) => { delete store[key]; },
  clear: () => { for (const k in store) delete store[k]; },
};

const OUTPUT_FORMAT = {
  resolution: '1920x1080',
  codec: 'h264',
  container: 'mp4',
  frame_rate: 24,
};

describe('RenderOrchestratorEngine', () => {
  let emitter: MockEventEmitter;
  let engine: RenderOrchestratorEngine;

  beforeEach(() => {
    (globalThis as any).localStorage.clear();
    emitter = new MockEventEmitter();
    engine = new RenderOrchestratorEngine(emitter);
  });

  it('should create a render job', () => {
    const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);

    expect(job.render_job_id.length).toBeGreaterThan(0);
    expect(job.status).toBe('queued');
    expect(job.progress_pct).toBe(0);
    expect(job.timeline_id).toBe('timeline-1');
    expect(job.retry_count).toBe(0);
    expect(emitter.events.length).toBe(1);
    expect(emitter.events[0].event_class).toBe('render_event');
    expect((emitter.events[0].payload as any).action).toBe('created');
  });

  it('should start a render', () => {
    const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);
    const started = engine.startRender(job.render_job_id);

    expect(started.status).toBe('rendering');
    expect(started.started_at).not.toBeNull();
    expect(emitter.events.length).toBe(2);
    expect((emitter.events[1].payload as any).action).toBe('started');
  });

  it('should complete render lifecycle', () => {
    const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);
    engine.startRender(job.render_job_id);
    const completed = engine.completeRender(job.render_job_id, 'output://rendered-file.mp4');

    expect(completed.status).toBe('completed');
    expect(completed.progress_pct).toBe(100);
    expect(completed.output_ref).toBe('output://rendered-file.mp4');
    expect(completed.completed_at).not.toBeNull();
    expect(emitter.events.length).toBe(3);
    expect((emitter.events[2].payload as any).action).toBe('completed');
  });

  it('should fail a render', () => {
    const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);
    engine.startRender(job.render_job_id);
    const failed = engine.failRender(job.render_job_id, 'Codec error: unsupported format');

    expect(failed.status).toBe('failed');
    expect(failed.error_detail).toBe('Codec error: unsupported format');
    expect(emitter.events.length).toBe(3);
    expect((emitter.events[2].payload as any).can_retry).toBe(true);
  });

  it('should handle fail and retry path', () => {
    const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT, 2);
    engine.startRender(job.render_job_id);
    engine.failRender(job.render_job_id, 'Transient error');

    const retried = engine.retryRender(job.render_job_id);
    expect(retried.status).toBe('queued');
    expect(retried.retry_count).toBe(1);
    expect(retried.error_detail).toBeNull();

    // Retry again after another failure
    engine.startRender(job.render_job_id);
    engine.failRender(job.render_job_id, 'Another error');
    const retried2 = engine.retryRender(job.render_job_id);
    expect(retried2.retry_count).toBe(2);

    // Third failure should exhaust retries
    engine.startRender(job.render_job_id);
    engine.failRender(job.render_job_id, 'Final error');
    expect(() => engine.retryRender(job.render_job_id)).toThrow('exhausted retries');
  });

  it('should emit events per state change', () => {
    const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);
    engine.startRender(job.render_job_id);
    engine.completeRender(job.render_job_id, 'output://file.mp4');

    // 3 state changes = 3 events
    expect(emitter.events.length).toBe(3);
    const actions = emitter.events.map(e => (e.payload as any).action);
    expect(actions[0]).toBe('created');
    expect(actions[1]).toBe('started');
    expect(actions[2]).toBe('completed');

    // All events should be render_event class
    expect(emitter.events.every(e => e.event_class === 'render_event')).toBe(true);
    expect(emitter.events.every(e => e.replayable_flag === true)).toBe(true);
  });

  it('should fail-closed on invalid transitions', () => {
    const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);

    // Cannot complete from queued
    expect(() => engine.completeRender(job.render_job_id, 'output://file.mp4'))
      .toThrow('Invalid render job transition');

    // Cannot fail from queued
    expect(() => engine.failRender(job.render_job_id, 'error'))
      .toThrow('Invalid render job transition');

    // Start, complete, then try to start again
    engine.startRender(job.render_job_id);
    engine.completeRender(job.render_job_id, 'output://file.mp4');

    expect(() => engine.startRender(job.render_job_id))
      .toThrow('Invalid render job transition');
  });

  it('should persist output reference', () => {
    const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);
    engine.startRender(job.render_job_id);
    engine.completeRender(job.render_job_id, 'output://rendered-file.mp4');

    // Retrieve and check
    const retrieved = engine.getRenderJob(job.render_job_id);
    expect(retrieved).toBeDefined();
    expect(retrieved!.output_ref).toBe('output://rendered-file.mp4');
    expect(retrieved!.status).toBe('completed');
    expect(retrieved!.progress_pct).toBe(100);

    // Verify stored in localStorage
    const raw = (globalThis as any).localStorage.getItem('cineos:render-orchestrator:jobs');
    expect(raw).not.toBeNull();
    const stored = JSON.parse(raw);
    expect(stored.length).toBe(1);
    expect(stored[0].output_ref).toBe('output://rendered-file.mp4');
  });
});
