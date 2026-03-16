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

import { describe, it, expect, beforeEach } from 'vitest';
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

const RENDER_JOB: RenderJobRef = {
  render_job_id: 'render-001',
  timeline_id: 'timeline-001',
  timeline_version_id: 'version-001',
  output_ref: 'output://file.mp4',
};

describe('OutputLineageTracker', () => {
  let tracker: OutputLineageTracker;

  beforeEach(() => {
    (globalThis as any).localStorage.clear();
    tracker = new OutputLineageTracker();
  });

  it('should build a lineage chain', () => {
    const lineage = tracker.buildLineage(
      RENDER_JOB,
      ['editorial-decision-001'],
      ['source-media-001'],
    );

    expect(lineage.lineage_id.length).toBeGreaterThan(0);
    expect(lineage.output_id).toBe('output://file.mp4');
    expect(lineage.output_type).toBe('render_output');
    expect(lineage.source_chain.length).toBe(5);
    expect(lineage.source_chain[0].entity_type).toBe('render_job');
    expect(lineage.source_chain[1].entity_type).toBe('timeline_version');
    expect(lineage.source_chain[2].entity_type).toBe('timeline');
    expect(lineage.source_chain[3].entity_type).toBe('editorial_decision');
    expect(lineage.source_chain[4].entity_type).toBe('source_media');
  });

  it('should verify completeness of a complete chain', () => {
    const lineage = tracker.buildLineage(
      RENDER_JOB,
      ['editorial-decision-001'],
      ['source-media-001'],
    );

    const verified = tracker.verifyLineage(lineage.lineage_id);
    expect(verified.complete).toBe(true);
    expect(verified.verified_at).not.toBeNull();
  });

  it('should trace chain back to source media and editorial decisions', () => {
    const lineage = tracker.buildLineage(
      RENDER_JOB,
      ['ed-001', 'ed-002'],
      ['media-001', 'media-002', 'media-003'],
    );

    // Should include all editorial decisions
    const editorialEntries = lineage.source_chain.filter(e => e.entity_type === 'editorial_decision');
    expect(editorialEntries.length).toBe(2);
    expect(editorialEntries[0].entity_id).toBe('ed-001');
    expect(editorialEntries[1].entity_id).toBe('ed-002');

    // Should include all source media
    const mediaEntries = lineage.source_chain.filter(e => e.entity_type === 'source_media');
    expect(mediaEntries.length).toBe(3);
    expect(mediaEntries[0].relationship).toBe('sourced_from');

    // Verify the chain links CDG entities
    const decisionEntries = lineage.source_chain.filter(e => e.relationship === 'decided_by');
    expect(decisionEntries.length).toBe(2);
  });

  it('should detect incomplete chains', () => {
    // No editorial decisions or source media
    const lineage = tracker.buildLineage(RENDER_JOB, [], []);
    expect(lineage.complete).toBe(false);

    const verified = tracker.verifyLineage(lineage.lineage_id);
    expect(verified.complete).toBe(false);

    // Only editorial, no source
    const lineage2 = tracker.buildLineage(RENDER_JOB, ['ed-001'], []);
    expect(lineage2.complete).toBe(false);
  });

  it('should get lineage for output', () => {
    const lineage = tracker.buildLineage(
      RENDER_JOB,
      ['ed-001'],
      ['media-001'],
    );

    const retrieved = tracker.getLineageForOutput('output://file.mp4');
    expect(retrieved).toBeDefined();
    expect(retrieved!.lineage_id).toBe(lineage.lineage_id);
    expect(retrieved!.source_chain.length).toBe(5);

    // Non-existent output
    const notFound = tracker.getLineageForOutput('output://nonexistent.mp4');
    expect(notFound).toBeUndefined();
  });
});
