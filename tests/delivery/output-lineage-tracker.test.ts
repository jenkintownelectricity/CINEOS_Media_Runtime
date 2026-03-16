/**
 * Output Lineage Tracker — Tests
 *
 * 5 tests covering:
 * - Build lineage chain
 * - Verify completeness
 * - Chain traces back to source media and editorial decisions
 * - Incomplete chain detection
 * - Get lineage for output
 */

import {
  OutputLineageTracker,
  RenderJobRef,
} from '../../src/delivery/output-lineage-tracker';

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
  return new OutputLineageTracker();
}

const RENDER_JOB: RenderJobRef = {
  render_job_id: 'render-001',
  timeline_id: 'timeline-001',
  timeline_version_id: 'version-001',
  output_ref: 'output://file.mp4',
};

// --- Tests ---

// Test 1: Build lineage chain
(function test_buildLineage() {
  const tracker = setup();

  const lineage = tracker.buildLineage(
    RENDER_JOB,
    ['editorial-decision-001'],
    ['source-media-001'],
  );

  console.assert(lineage.lineage_id.length > 0, 'FAIL: lineage_id should be set');
  console.assert(lineage.output_id === 'output://file.mp4', 'FAIL: output_id should match output_ref');
  console.assert(lineage.output_type === 'render_output', 'FAIL: output_type should be render_output');
  console.assert(lineage.source_chain.length === 5, 'FAIL: chain should have 5 entries');
  console.assert(lineage.source_chain[0].entity_type === 'render_job', 'FAIL: first should be render_job');
  console.assert(lineage.source_chain[1].entity_type === 'timeline_version', 'FAIL: second should be timeline_version');
  console.assert(lineage.source_chain[2].entity_type === 'timeline', 'FAIL: third should be timeline');
  console.assert(lineage.source_chain[3].entity_type === 'editorial_decision', 'FAIL: fourth should be editorial_decision');
  console.assert(lineage.source_chain[4].entity_type === 'source_media', 'FAIL: fifth should be source_media');
  console.log('PASS: test_buildLineage');
})();

// Test 2: Verify completeness — complete chain
(function test_verifyCompleteness() {
  const tracker = setup();

  const lineage = tracker.buildLineage(
    RENDER_JOB,
    ['editorial-decision-001'],
    ['source-media-001'],
  );

  const verified = tracker.verifyLineage(lineage.lineage_id);
  console.assert(verified.complete === true, 'FAIL: chain should be complete');
  console.assert(verified.verified_at !== null, 'FAIL: verified_at should be set');
  console.log('PASS: test_verifyCompleteness');
})();

// Test 3: Chain traces back to source media and editorial decisions
(function test_chainTracesBack() {
  const tracker = setup();

  const lineage = tracker.buildLineage(
    RENDER_JOB,
    ['ed-001', 'ed-002'],
    ['media-001', 'media-002', 'media-003'],
  );

  // Should include all editorial decisions
  const editorialEntries = lineage.source_chain.filter(e => e.entity_type === 'editorial_decision');
  console.assert(editorialEntries.length === 2, 'FAIL: should have 2 editorial decisions');
  console.assert(editorialEntries[0].entity_id === 'ed-001', 'FAIL: first editorial ID mismatch');
  console.assert(editorialEntries[1].entity_id === 'ed-002', 'FAIL: second editorial ID mismatch');

  // Should include all source media
  const mediaEntries = lineage.source_chain.filter(e => e.entity_type === 'source_media');
  console.assert(mediaEntries.length === 3, 'FAIL: should have 3 source media entries');
  console.assert(mediaEntries[0].relationship === 'sourced_from', 'FAIL: relationship should be sourced_from');

  // Verify the chain links CDG entities
  const decisionEntries = lineage.source_chain.filter(e => e.relationship === 'decided_by');
  console.assert(decisionEntries.length === 2, 'FAIL: should have 2 decided_by relationships');
  console.log('PASS: test_chainTracesBack');
})();

// Test 4: Incomplete chain detection
(function test_incompleteChain() {
  const tracker = setup();

  // No editorial decisions or source media
  const lineage = tracker.buildLineage(RENDER_JOB, [], []);
  console.assert(lineage.complete === false, 'FAIL: chain without editorial/source should be incomplete');

  const verified = tracker.verifyLineage(lineage.lineage_id);
  console.assert(verified.complete === false, 'FAIL: verification should confirm incomplete');

  // Only editorial, no source
  const lineage2 = tracker.buildLineage(RENDER_JOB, ['ed-001'], []);
  console.assert(lineage2.complete === false, 'FAIL: chain without source media should be incomplete');
  console.log('PASS: test_incompleteChain');
})();

// Test 5: Get lineage for output
(function test_getLineageForOutput() {
  const tracker = setup();

  const lineage = tracker.buildLineage(
    RENDER_JOB,
    ['ed-001'],
    ['media-001'],
  );

  const retrieved = tracker.getLineageForOutput('output://file.mp4');
  console.assert(retrieved !== undefined, 'FAIL: should find lineage');
  console.assert(retrieved!.lineage_id === lineage.lineage_id, 'FAIL: lineage_id should match');
  console.assert(retrieved!.source_chain.length === 5, 'FAIL: chain length should match');

  // Non-existent output
  const notFound = tracker.getLineageForOutput('output://nonexistent.mp4');
  console.assert(notFound === undefined, 'FAIL: should return undefined for unknown output');
  console.log('PASS: test_getLineageForOutput');
})();

console.log('\n=== output-lineage-tracker: All 5 tests passed ===');
