// distributed-render-coordinator.ts — CINEOS Media Runtime
// Wave 8: Distributed render coordination across network nodes
// Coordinates render jobs, aggregates results, enforces capability checks
// All cross-project render results are non-authoritative with provenance tracking

// ─── Types ───────────────────────────────────────────────────────────────────

export interface RenderJob {
  job_id: string;
  project_id: string;
  render_type: string;
  input_entities: string[];
  requested_by: string;
  capability_ref: string;
  created_at: string;
}

export interface NodeAssignment {
  node_id: string;
  node_type: 'project_node';
  work_segment: string;
  input_slice: string[];
  status: 'pending' | 'running' | 'completed' | 'failed';
}

export interface DistributedRenderPlan {
  plan_id: string;
  job_id: string;
  node_assignments: NodeAssignment[];
  total_nodes: number;
  estimated_completion: string;
}

export interface NodeResult {
  node_id: string;
  source_project_id: string;
  output_ref: string;
  status: 'success' | 'failed';
  duration_ms: number;
}

export interface RenderAggregation {
  job_id: string;
  plan_id: string;
  node_results: NodeResult[];
  authoritative: false;
  completed_at: string;
  provenance_chain: string[];
}

export interface NetworkNode {
  node_id: string;
  node_type: string;
  studio_id: string;
  project_id: string | null;
  capabilities: string[];
}

// ─── Coordinator ─────────────────────────────────────────────────────────────

export class DistributedRenderCoordinator {
  private readonly projectId: string;
  private readonly studioId: string;
  private planCounter = 0;

  constructor(projectId: string, studioId: string) {
    this.projectId = projectId;
    this.studioId = studioId;
  }

  /**
   * Creates a distributed render plan across available network nodes.
   * Fail-closed: nodes without the required capability are excluded.
   * Throws if no eligible nodes remain after capability filtering.
   */
  planDistributedRender(
    job: RenderJob,
    availableNodes: NetworkNode[],
  ): DistributedRenderPlan {
    // Fail-closed: only nodes with 'project' capability are eligible for render work
    // Per KERN-CINEOS-NETWORK-FEDERATION, valid node capabilities are: read, project, aggregate
    const eligibleNodes = availableNodes.filter(
      (node) => node.capabilities.includes('project'),
    );

    if (eligibleNodes.length === 0) {
      throw new Error(
        `[DistributedRenderCoordinator] No eligible nodes with 'project' capability for job ${job.job_id}. Fail-closed.`,
      );
    }

    // Validate the requester's capability before planning
    if (!this.validateRenderCapability(job.requested_by, job.capability_ref)) {
      throw new Error(
        `[DistributedRenderCoordinator] Render capability validation failed for requester '${job.requested_by}' with ref '${job.capability_ref}'. Fail-closed.`,
      );
    }

    // Distribute input entities across eligible nodes
    const slices = this.distributeSlices(job.input_entities, eligibleNodes.length);

    const assignments: NodeAssignment[] = eligibleNodes.map((node, idx) => ({
      node_id: node.node_id,
      node_type: 'project_node' as const,
      work_segment: `segment-${idx}`,
      input_slice: slices[idx] ?? [],
      status: 'pending' as const,
    }));

    this.planCounter += 1;
    const planId = `plan-${this.projectId}-${this.planCounter}-${Date.now()}`;

    // Estimate completion: rough heuristic based on entity count and node count
    const estimatedMs = Math.ceil(job.input_entities.length / eligibleNodes.length) * 100;
    const estimatedCompletion = new Date(Date.now() + estimatedMs).toISOString();

    return {
      plan_id: planId,
      job_id: job.job_id,
      node_assignments: assignments,
      total_nodes: eligibleNodes.length,
      estimated_completion: estimatedCompletion,
    };
  }

  /**
   * Aggregates results from distributed render nodes.
   * Cross-project results are always marked authoritative: false.
   * Provenance chain tracks every contributing node and source project.
   */
  aggregateResults(
    plan: DistributedRenderPlan,
    results: NodeResult[],
  ): RenderAggregation {
    // Build provenance chain: record every node and source project that contributed
    const provenanceChain: string[] = [];

    provenanceChain.push(`coordinator:${this.projectId}`);
    provenanceChain.push(`plan:${plan.plan_id}`);

    for (const result of results) {
      provenanceChain.push(
        `node:${result.node_id}:project:${result.source_project_id}:status:${result.status}`,
      );
    }

    return {
      job_id: plan.job_id,
      plan_id: plan.plan_id,
      node_results: results,
      authoritative: false as const, // Cross-project results are never authoritative
      completed_at: new Date().toISOString(),
      provenance_chain: provenanceChain,
    };
  }

  /**
   * Validates that a requester holds the referenced render capability.
   * Fail-closed: returns false if the capability ref is empty or malformed.
   */
  validateRenderCapability(requesterId: string, capabilityRef: string): boolean {
    // Fail-closed: reject empty or missing identifiers
    if (!requesterId || requesterId.trim().length === 0) {
      return false;
    }
    if (!capabilityRef || capabilityRef.trim().length === 0) {
      return false;
    }

    // Capability ref must follow the expected format: cap:<type>:<scope>
    const capPattern = /^cap:[a-zA-Z_]+:[a-zA-Z0-9_*-]+$/;
    if (!capPattern.test(capabilityRef)) {
      return false;
    }

    // The capability must include 'render' in the type segment
    const segments = capabilityRef.split(':');
    if (segments.length < 3) {
      return false;
    }
    const capType = segments[1];
    if (!capType.includes('render')) {
      return false;
    }

    return true;
  }

  // ─── Private helpers ─────────────────────────────────────────────────────

  /**
   * Distributes input entities as evenly as possible across N slices.
   */
  private distributeSlices(entities: string[], nodeCount: number): string[][] {
    const slices: string[][] = Array.from({ length: nodeCount }, () => []);
    for (let i = 0; i < entities.length; i++) {
      slices[i % nodeCount].push(entities[i]);
    }
    return slices;
  }
}
