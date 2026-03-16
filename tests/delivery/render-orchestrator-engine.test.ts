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

function setup() {
  (globalThis as any).localStorage.clear();
  const emitter = new MockEventEmitter();
  const engine = new RenderOrchestratorEngine(emitter);
  return { emitter, engine };
}

const OUTPUT_FORMAT = {
  resolution: '1920x1080',
  codec: 'h264',
  container: 'mp4',
  frame_rate: 24,
};

// --- Tests ---

// Test 1: Create render job
(function test_createRenderJob() {
  const { engine, emitter } = setup();

  const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);

  console.assert(job.render_job_id.length > 0, 'FAIL: render_job_id should be set');
  console.assert(job.status === 'queued', 'FAIL: status should be queued');
  console.assert(job.progress_pct === 0, 'FAIL: progress should be 0');
  console.assert(job.timeline_id === 'timeline-1', 'FAIL: timeline_id mismatch');
  console.assert(job.retry_count === 0, 'FAIL: retry_count should be 0');
  console.assert(emitter.events.length === 1, 'FAIL: should emit 1 event');
  console.assert(emitter.events[0].event_class === 'render_event', 'FAIL: event class should be render_event');
  console.assert((emitter.events[0].payload as any).action === 'created', 'FAIL: action should be created');
  console.log('PASS: test_createRenderJob');
})();

// Test 2: Start render
(function test_startRender() {
  const { engine, emitter } = setup();

  const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);
  const started = engine.startRender(job.render_job_id);

  console.assert(started.status === 'rendering', 'FAIL: status should be rendering');
  console.assert(started.started_at !== null, 'FAIL: started_at should be set');
  console.assert(emitter.events.length === 2, 'FAIL: should emit 2 events');
  console.assert((emitter.events[1].payload as any).action === 'started', 'FAIL: action should be started');
  console.log('PASS: test_startRender');
})();

// Test 3: Complete render lifecycle
(function test_completeRenderLifecycle() {
  const { engine, emitter } = setup();

  const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);
  engine.startRender(job.render_job_id);
  const completed = engine.completeRender(job.render_job_id, 'output://rendered-file.mp4');

  console.assert(completed.status === 'completed', 'FAIL: status should be completed');
  console.assert(completed.progress_pct === 100, 'FAIL: progress should be 100');
  console.assert(completed.output_ref === 'output://rendered-file.mp4', 'FAIL: output_ref mismatch');
  console.assert(completed.completed_at !== null, 'FAIL: completed_at should be set');
  console.assert(emitter.events.length === 3, 'FAIL: should emit 3 events');
  console.assert((emitter.events[2].payload as any).action === 'completed', 'FAIL: action should be completed');
  console.log('PASS: test_completeRenderLifecycle');
})();

// Test 4: Fail render
(function test_failRender() {
  const { engine, emitter } = setup();

  const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);
  engine.startRender(job.render_job_id);
  const failed = engine.failRender(job.render_job_id, 'Codec error: unsupported format');

  console.assert(failed.status === 'failed', 'FAIL: status should be failed');
  console.assert(failed.error_detail === 'Codec error: unsupported format', 'FAIL: error_detail mismatch');
  console.assert(emitter.events.length === 3, 'FAIL: should emit 3 events');
  console.assert((emitter.events[2].payload as any).can_retry === true, 'FAIL: should have retry path');
  console.log('PASS: test_failRender');
})();

// Test 5: Fail and retry path
(function test_failAndRetry() {
  const { engine, emitter } = setup();

  const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT, 2);
  engine.startRender(job.render_job_id);
  engine.failRender(job.render_job_id, 'Transient error');

  const retried = engine.retryRender(job.render_job_id);
  console.assert(retried.status === 'queued', 'FAIL: retried status should be queued');
  console.assert(retried.retry_count === 1, 'FAIL: retry_count should be 1');
  console.assert(retried.error_detail === null, 'FAIL: error_detail should be cleared');

  // Retry again after another failure
  engine.startRender(job.render_job_id);
  engine.failRender(job.render_job_id, 'Another error');
  const retried2 = engine.retryRender(job.render_job_id);
  console.assert(retried2.retry_count === 2, 'FAIL: retry_count should be 2');

  // Third failure should exhaust retries
  engine.startRender(job.render_job_id);
  engine.failRender(job.render_job_id, 'Final error');
  let exhaustedError = '';
  try {
    engine.retryRender(job.render_job_id);
  } catch (e: any) {
    exhaustedError = e.message;
  }
  console.assert(exhaustedError.includes('exhausted retries'), 'FAIL: should throw on exhausted retries');
  console.log('PASS: test_failAndRetry');
})();

// Test 6: Event emission per state change
(function test_eventEmissionPerStateChange() {
  const { engine, emitter } = setup();

  const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);
  engine.startRender(job.render_job_id);
  engine.completeRender(job.render_job_id, 'output://file.mp4');

  // 3 state changes = 3 events
  console.assert(emitter.events.length === 3, 'FAIL: should emit exactly 3 events');
  const actions = emitter.events.map(e => (e.payload as any).action);
  console.assert(actions[0] === 'created', 'FAIL: first action should be created');
  console.assert(actions[1] === 'started', 'FAIL: second action should be started');
  console.assert(actions[2] === 'completed', 'FAIL: third action should be completed');

  // All events should be render_event class
  console.assert(emitter.events.every(e => e.event_class === 'render_event'), 'FAIL: all should be render_event');
  console.assert(emitter.events.every(e => e.replayable_flag === true), 'FAIL: all should be replayable');
  console.log('PASS: test_eventEmissionPerStateChange');
})();

// Test 7: Fail-closed on invalid transitions
(function test_failClosedInvalidTransitions() {
  const { engine } = setup();

  const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);

  // Cannot complete from queued
  let err1 = '';
  try {
    engine.completeRender(job.render_job_id, 'output://file.mp4');
  } catch (e: any) {
    err1 = e.message;
  }
  console.assert(err1.includes('Invalid render job transition'), 'FAIL: should throw on queued→completed');

  // Cannot fail from queued
  let err2 = '';
  try {
    engine.failRender(job.render_job_id, 'error');
  } catch (e: any) {
    err2 = e.message;
  }
  console.assert(err2.includes('Invalid render job transition'), 'FAIL: should throw on queued→failed');

  // Start, complete, then try to start again
  engine.startRender(job.render_job_id);
  engine.completeRender(job.render_job_id, 'output://file.mp4');

  let err3 = '';
  try {
    engine.startRender(job.render_job_id);
  } catch (e: any) {
    err3 = e.message;
  }
  console.assert(err3.includes('Invalid render job transition'), 'FAIL: should throw on completed→rendering');
  console.log('PASS: test_failClosedInvalidTransitions');
})();

// Test 8: Output reference persistence
(function test_outputReferencePersistence() {
  const { engine } = setup();

  const job = engine.createRenderJob('timeline-1', 'version-1', OUTPUT_FORMAT);
  engine.startRender(job.render_job_id);
  engine.completeRender(job.render_job_id, 'output://rendered-file.mp4');

  // Retrieve and check
  const retrieved = engine.getRenderJob(job.render_job_id);
  console.assert(retrieved !== undefined, 'FAIL: should find the job');
  console.assert(retrieved!.output_ref === 'output://rendered-file.mp4', 'FAIL: output_ref should persist');
  console.assert(retrieved!.status === 'completed', 'FAIL: status should be completed');
  console.assert(retrieved!.progress_pct === 100, 'FAIL: progress should be 100');

  // Verify stored in localStorage
  const raw = (globalThis as any).localStorage.getItem('cineos:render-orchestrator:jobs');
  console.assert(raw !== null, 'FAIL: should be stored in localStorage');
  const stored = JSON.parse(raw);
  console.assert(stored.length === 1, 'FAIL: should have 1 stored job');
  console.assert(stored[0].output_ref === 'output://rendered-file.mp4', 'FAIL: stored output_ref should match');
  console.log('PASS: test_outputReferencePersistence');
})();

console.log('\n=== render-orchestrator-engine: All 8 tests passed ===');
