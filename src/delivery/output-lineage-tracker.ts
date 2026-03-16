/**
 * Output Lineage Tracker
 *
 * Constructs and verifies full provenance chains from delivered output
 * back through render → timeline → editorial decisions → source media.
 * Lineage links back to CDG entities for decision provenance.
 *
 * Classification: foundational lineage tracking — not production DAM.
 * Persistence: localStorage-backed.
 */

export interface SourceChainEntry {
  entity_type: string;
  entity_id: string;
  relationship: string;
}

export interface OutputLineage {
  lineage_id: string;
  output_id: string;
  output_type: 'render_output' | 'delivery_package' | 'proxy' | 'thumbnail';
  source_chain: SourceChainEntry[];
  complete: boolean;
  verified_at: string | null;
}

export interface RenderJobRef {
  render_job_id: string;
  timeline_id: string;
  timeline_version_id: string;
  output_ref: string | null;
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

const STORAGE_KEY = 'cineos:output-lineage:records';

export class OutputLineageTracker {
  private lineages: Map<string, OutputLineage> = new Map();

  constructor() {
    this.loadFromStorage();
  }

  private loadFromStorage(): void {
    try {
      if (typeof localStorage !== 'undefined') {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (raw) {
          const arr: OutputLineage[] = JSON.parse(raw);
          for (const l of arr) {
            this.lineages.set(l.lineage_id, l);
          }
        }
      }
    } catch {
      // Storage unavailable
    }
  }

  private persistToStorage(): void {
    try {
      if (typeof localStorage !== 'undefined') {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(Array.from(this.lineages.values())));
      }
    } catch (e) {
      throw new Error(`Lineage persistence failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  /**
   * Build a lineage chain for a render job output.
   * Constructs: output → render_job → timeline_version → timeline → editorial_decision → source_media
   *
   * @param renderJob - Reference to the render job
   * @param editorialDecisionIds - CDG entity IDs for editorial decisions linked to this timeline
   * @param sourceMediaIds - Source media asset IDs used in the timeline
   */
  buildLineage(
    renderJob: RenderJobRef,
    editorialDecisionIds: string[] = [],
    sourceMediaIds: string[] = [],
  ): OutputLineage {
    if (!renderJob.render_job_id) throw new Error('render_job_id is required');
    if (!renderJob.timeline_id) throw new Error('timeline_id is required');

    const chain: SourceChainEntry[] = [];

    // Render job → output
    chain.push({
      entity_type: 'render_job',
      entity_id: renderJob.render_job_id,
      relationship: 'rendered_by',
    });

    // Timeline version
    chain.push({
      entity_type: 'timeline_version',
      entity_id: renderJob.timeline_version_id,
      relationship: 'rendered_from',
    });

    // Timeline
    chain.push({
      entity_type: 'timeline',
      entity_id: renderJob.timeline_id,
      relationship: 'version_of',
    });

    // Editorial decisions (CDG entities)
    for (const decisionId of editorialDecisionIds) {
      chain.push({
        entity_type: 'editorial_decision',
        entity_id: decisionId,
        relationship: 'decided_by',
      });
    }

    // Source media
    for (const mediaId of sourceMediaIds) {
      chain.push({
        entity_type: 'source_media',
        entity_id: mediaId,
        relationship: 'sourced_from',
      });
    }

    const isComplete = editorialDecisionIds.length > 0 && sourceMediaIds.length > 0;

    const lineage: OutputLineage = {
      lineage_id: generateId(),
      output_id: renderJob.output_ref ?? renderJob.render_job_id,
      output_type: 'render_output',
      source_chain: chain,
      complete: isComplete,
      verified_at: null,
    };

    this.lineages.set(lineage.lineage_id, lineage);
    this.persistToStorage();
    return { ...lineage, source_chain: [...chain] };
  }

  /**
   * Verify a lineage chain for completeness.
   * Complete means: chain includes render_job, timeline, editorial_decision, and source_media.
   */
  verifyLineage(lineageId: string): OutputLineage {
    const lineage = this.lineages.get(lineageId);
    if (!lineage) throw new Error(`Lineage not found: ${lineageId}`);

    const entityTypes = new Set(lineage.source_chain.map(e => e.entity_type));
    const requiredTypes = ['render_job', 'timeline_version', 'timeline', 'editorial_decision', 'source_media'];
    const isComplete = requiredTypes.every(t => entityTypes.has(t));

    lineage.complete = isComplete;
    lineage.verified_at = new Date().toISOString();

    this.persistToStorage();
    return { ...lineage, source_chain: [...lineage.source_chain] };
  }

  /**
   * Get lineage for a specific output ID.
   */
  getLineageForOutput(outputId: string): OutputLineage | undefined {
    for (const lineage of this.lineages.values()) {
      if (lineage.output_id === outputId) {
        return { ...lineage, source_chain: [...lineage.source_chain] };
      }
    }
    return undefined;
  }

  /**
   * Get lineage by ID.
   */
  getLineage(lineageId: string): OutputLineage | undefined {
    const lineage = this.lineages.get(lineageId);
    return lineage ? { ...lineage, source_chain: [...lineage.source_chain] } : undefined;
  }

  /**
   * Get all lineages.
   */
  getAllLineages(): OutputLineage[] {
    return Array.from(this.lineages.values()).map(l => ({
      ...l,
      source_chain: [...l.source_chain],
    }));
  }
}
