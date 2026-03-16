/**
 * Wave 8 Network Tests - DistributedRenderCoordinator
 *
 * Validates kernel law enforcement:
 *   - Render aggregation has authoritative: false
 *   - Provenance chain tracked for all contributing nodes
 *   - Fail-closed on missing capabilities
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  DistributedRenderCoordinator,
} from '../../src/delivery/distributed-render-coordinator';
import type {
  RenderJob,
  NetworkNode,
  NodeResult,
} from '../../src/delivery/distributed-render-coordinator';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const projectId = 'proj-001';
const studioId = 'stid-00000000-0000-0000-0000-000000000001';

function makeCoordinator(): DistributedRenderCoordinator {
  return new DistributedRenderCoordinator(projectId, studioId);
}

function makeJob(overrides: Partial<RenderJob> = {}): RenderJob {
  return {
    job_id: 'job-001',
    project_id: projectId,
    render_type: 'final_output',
    input_entities: ['entity-1', 'entity-2', 'entity-3', 'entity-4'],
    requested_by: 'huid-00000000-0000-0000-0000-000000000001',
    capability_ref: 'cap:render:proj_001',
    created_at: new Date().toISOString(),
    ...overrides,
  };
}

function makeRenderNode(overrides: Partial<NetworkNode> = {}): NetworkNode {
  return {
    node_id: 'render-node-1',
    node_type: 'project_node',
    studio_id: studioId,
    project_id: 'proj-001',
    capabilities: ['project'],
    ...overrides,
  };
}

function makeNodeResult(overrides: Partial<NodeResult> = {}): NodeResult {
  return {
    node_id: 'render-node-1',
    source_project_id: 'proj-001',
    output_ref: 'output:render-node-1:job-001',
    status: 'success',
    duration_ms: 100,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('DistributedRenderCoordinator', () => {
  let coordinator: DistributedRenderCoordinator;

  beforeEach(() => {
    coordinator = makeCoordinator();
  });

  // -------------------------------------------------------------------------
  // Render aggregation has authoritative: false
  // -------------------------------------------------------------------------

  describe('render aggregation has authoritative: false', () => {
    it('aggregateResults always returns authoritative: false', () => {
      const nodes = [makeRenderNode()];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const results = [makeNodeResult()];
      const aggregation = coordinator.aggregateResults(plan, results);
      expect(aggregation.authoritative).toBe(false);
    });

    it('authoritative is literally false, not falsy', () => {
      const nodes = [makeRenderNode()];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const aggregation = coordinator.aggregateResults(plan, [makeNodeResult()]);
      expect(aggregation.authoritative).toStrictEqual(false);
    });

    it('aggregation includes correct job_id', () => {
      const nodes = [makeRenderNode()];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const aggregation = coordinator.aggregateResults(plan, [makeNodeResult()]);
      expect(aggregation.job_id).toBe('job-001');
    });

    it('aggregation includes plan_id', () => {
      const nodes = [makeRenderNode()];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const aggregation = coordinator.aggregateResults(plan, [makeNodeResult()]);
      expect(aggregation.plan_id).toBe(plan.plan_id);
    });

    it('aggregation includes completed_at timestamp', () => {
      const nodes = [makeRenderNode()];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const aggregation = coordinator.aggregateResults(plan, [makeNodeResult()]);
      expect(aggregation.completed_at).toBeDefined();
      expect(typeof aggregation.completed_at).toBe('string');
    });

    it('aggregation includes all node results', () => {
      const nodes = [
        makeRenderNode({ node_id: 'rn-1' }),
        makeRenderNode({ node_id: 'rn-2' }),
      ];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const results = [
        makeNodeResult({ node_id: 'rn-1' }),
        makeNodeResult({ node_id: 'rn-2', source_project_id: 'proj-002' }),
      ];
      const aggregation = coordinator.aggregateResults(plan, results);
      expect(aggregation.node_results).toHaveLength(2);
    });
  });

  // -------------------------------------------------------------------------
  // Provenance chain tracked
  // -------------------------------------------------------------------------

  describe('provenance chain tracked', () => {
    it('provenance chain includes coordinator entry', () => {
      const nodes = [makeRenderNode()];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const aggregation = coordinator.aggregateResults(plan, [makeNodeResult()]);
      expect(aggregation.provenance_chain).toContain(`coordinator:${projectId}`);
    });

    it('provenance chain includes plan reference', () => {
      const nodes = [makeRenderNode()];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const aggregation = coordinator.aggregateResults(plan, [makeNodeResult()]);
      const planEntry = aggregation.provenance_chain.find(e => e.startsWith('plan:'));
      expect(planEntry).toBeDefined();
      expect(planEntry).toContain(plan.plan_id);
    });

    it('provenance chain includes all contributing node entries', () => {
      const nodes = [
        makeRenderNode({ node_id: 'rn-1' }),
        makeRenderNode({ node_id: 'rn-2' }),
      ];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const results = [
        makeNodeResult({ node_id: 'rn-1', source_project_id: 'proj-001' }),
        makeNodeResult({ node_id: 'rn-2', source_project_id: 'proj-002' }),
      ];
      const aggregation = coordinator.aggregateResults(plan, results);

      // coordinator + plan + 2 node entries = 4
      expect(aggregation.provenance_chain.length).toBe(4);
      expect(aggregation.provenance_chain.some(e => e.includes('rn-1'))).toBe(true);
      expect(aggregation.provenance_chain.some(e => e.includes('rn-2'))).toBe(true);
    });

    it('provenance chain records node status for successful results', () => {
      const nodes = [makeRenderNode()];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const results = [makeNodeResult({ status: 'success' })];
      const aggregation = coordinator.aggregateResults(plan, results);
      expect(aggregation.provenance_chain.some(e => e.includes('status:success'))).toBe(true);
    });

    it('provenance chain records node status for failed results', () => {
      const nodes = [makeRenderNode()];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const results = [makeNodeResult({ status: 'failed' })];
      const aggregation = coordinator.aggregateResults(plan, results);
      expect(aggregation.provenance_chain.some(e => e.includes('status:failed'))).toBe(true);
    });

    it('provenance chain records source project for each node', () => {
      const nodes = [makeRenderNode({ node_id: 'rn-1' })];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      const results = [makeNodeResult({ node_id: 'rn-1', source_project_id: 'proj-special' })];
      const aggregation = coordinator.aggregateResults(plan, results);
      expect(aggregation.provenance_chain.some(e => e.includes('proj-special'))).toBe(true);
    });
  });

  // -------------------------------------------------------------------------
  // Fail-closed on missing capabilities
  // -------------------------------------------------------------------------

  describe('fail-closed on missing capabilities', () => {
    it('throws when no nodes have project capability', () => {
      const nodes = [makeRenderNode({ capabilities: ['read'] })];
      expect(() =>
        coordinator.planDistributedRender(makeJob(), nodes),
      ).toThrow(/No eligible nodes/);
    });

    it('throws when available nodes array is empty', () => {
      expect(() =>
        coordinator.planDistributedRender(makeJob(), []),
      ).toThrow(/No eligible nodes/);
    });

    it('throws when nodes only have non-project capabilities', () => {
      const nodes = [
        makeRenderNode({ node_id: 'n1', capabilities: ['read'] }),
        makeRenderNode({ node_id: 'n2', capabilities: ['aggregate'] }),
      ];
      expect(() =>
        coordinator.planDistributedRender(makeJob(), nodes),
      ).toThrow(/No eligible nodes/);
    });

    it('throws when requester has empty capability_ref', () => {
      const nodes = [makeRenderNode()];
      expect(() =>
        coordinator.planDistributedRender(makeJob({ capability_ref: '' }), nodes),
      ).toThrow(/Render capability validation failed/);
    });

    it('throws when requester has empty requested_by', () => {
      const nodes = [makeRenderNode()];
      expect(() =>
        coordinator.planDistributedRender(makeJob({ requested_by: '' }), nodes),
      ).toThrow(/Render capability validation failed/);
    });

    it('throws when capability_ref does not contain render', () => {
      const nodes = [makeRenderNode()];
      expect(() =>
        coordinator.planDistributedRender(
          makeJob({ capability_ref: 'cap:read:studio_a' }),
          nodes,
        ),
      ).toThrow(/Render capability validation failed/);
    });

    it('throws when capability_ref has invalid format', () => {
      const nodes = [makeRenderNode()];
      expect(() =>
        coordinator.planDistributedRender(
          makeJob({ capability_ref: 'invalid-format' }),
          nodes,
        ),
      ).toThrow(/Render capability validation failed/);
    });

    it('filters out nodes without project capability', () => {
      const nodes = [
        makeRenderNode({ node_id: 'rn-1', capabilities: ['project'] }),
        makeRenderNode({ node_id: 'rn-2', capabilities: ['read'] }),
        makeRenderNode({ node_id: 'rn-3', capabilities: ['project'] }),
      ];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      expect(plan.total_nodes).toBe(2);
      const assignedNodeIds = plan.node_assignments.map(a => a.node_id);
      expect(assignedNodeIds).toContain('rn-1');
      expect(assignedNodeIds).toContain('rn-3');
      expect(assignedNodeIds).not.toContain('rn-2');
    });
  });

  // -------------------------------------------------------------------------
  // Plan structure
  // -------------------------------------------------------------------------

  describe('plan structure', () => {
    it('creates a plan with correct job_id', () => {
      const plan = coordinator.planDistributedRender(makeJob(), [makeRenderNode()]);
      expect(plan.job_id).toBe('job-001');
    });

    it('assigns work segments to nodes', () => {
      const nodes = [
        makeRenderNode({ node_id: 'rn-1' }),
        makeRenderNode({ node_id: 'rn-2' }),
      ];
      const plan = coordinator.planDistributedRender(makeJob(), nodes);
      expect(plan.node_assignments).toHaveLength(2);
      expect(plan.node_assignments[0].work_segment).toBe('segment-0');
      expect(plan.node_assignments[1].work_segment).toBe('segment-1');
    });

    it('distributes input entities across nodes evenly', () => {
      const nodes = [
        makeRenderNode({ node_id: 'rn-1' }),
        makeRenderNode({ node_id: 'rn-2' }),
      ];
      const job = makeJob({ input_entities: ['e-1', 'e-2', 'e-3', 'e-4'] });
      const plan = coordinator.planDistributedRender(job, nodes);

      const totalEntities = plan.node_assignments.reduce(
        (sum, a) => sum + a.input_slice.length, 0,
      );
      expect(totalEntities).toBe(4);
      expect(plan.node_assignments[0].input_slice.length).toBe(2);
      expect(plan.node_assignments[1].input_slice.length).toBe(2);
    });

    it('all assignments start with pending status', () => {
      const plan = coordinator.planDistributedRender(makeJob(), [makeRenderNode()]);
      for (const assignment of plan.node_assignments) {
        expect(assignment.status).toBe('pending');
      }
    });

    it('includes estimated_completion timestamp', () => {
      const plan = coordinator.planDistributedRender(makeJob(), [makeRenderNode()]);
      expect(plan.estimated_completion).toBeDefined();
      expect(typeof plan.estimated_completion).toBe('string');
    });

    it('plan_id is unique per call', () => {
      const nodes = [makeRenderNode()];
      const plan1 = coordinator.planDistributedRender(makeJob(), nodes);
      const plan2 = coordinator.planDistributedRender(makeJob({ job_id: 'job-002' }), nodes);
      expect(plan1.plan_id).not.toBe(plan2.plan_id);
    });
  });

  // -------------------------------------------------------------------------
  // validateRenderCapability
  // -------------------------------------------------------------------------

  describe('validateRenderCapability', () => {
    it('returns true for valid render capability', () => {
      expect(coordinator.validateRenderCapability('user-1', 'cap:render:proj_001')).toBe(true);
    });

    it('returns false for empty requesterId', () => {
      expect(coordinator.validateRenderCapability('', 'cap:render:proj_001')).toBe(false);
    });

    it('returns false for whitespace-only requesterId', () => {
      expect(coordinator.validateRenderCapability('   ', 'cap:render:proj_001')).toBe(false);
    });

    it('returns false for empty capabilityRef', () => {
      expect(coordinator.validateRenderCapability('user-1', '')).toBe(false);
    });

    it('returns false for non-render capability type', () => {
      expect(coordinator.validateRenderCapability('user-1', 'cap:read:studio_a')).toBe(false);
    });

    it('returns false for malformed capability ref', () => {
      expect(coordinator.validateRenderCapability('user-1', 'not-a-cap')).toBe(false);
    });

    it('returns false for capability ref with too few segments', () => {
      expect(coordinator.validateRenderCapability('user-1', 'cap:render')).toBe(false);
    });
  });
});
