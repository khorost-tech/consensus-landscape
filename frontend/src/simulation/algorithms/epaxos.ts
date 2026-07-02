import { ConsensusAlgorithm } from './interface';
import {
  NodeState, NodeId, Message, Action, ClusterConfig, TimeoutType, LogEntry, ClientRequest,
} from '../types';

/**
 * EPaxos (Egalitarian Paxos) — leaderless consensus with optimal latency.
 *
 * Any node ("replica") can propose. Fast path (1 RTT) when no conflicts.
 * Slow path (2 RTT) when conflicts require explicit dependency resolution.
 *
 * Key differences from other algorithms visible in simulation:
 * - No leader — all nodes are equal blue "R" replicas
 * - Most commits in 1 RTT (fast path) vs 2 RTT (Paxos) or leader bottleneck (Raft)
 * - Conflicts trigger visible slow-path messages (purple diamonds)
 * - Dependency tracking between concurrent commands
 *
 * Simplified for educational clarity:
 * - "Conflict" = two uncommitted commands from different replicas
 * - Dependency-aware materialization is simplified and uses SCC batching
 */

interface Instance {
  entryId: string;
  requestId: string;
  command: string;
  status: 'pre-accepted' | 'accepted' | 'committed';
  seq: number;
  deps: string[];         // dependency instance IDs
  indexHint: number | null;
  ballot: number;
  leaderNode: NodeId;
  preAcceptOks: number;
  acceptOks: number;
  allDepsMatch: boolean;
}

const STATUS_RANK: Record<Instance['status'], number> = {
  'pre-accepted': 0,
  accepted: 1,
  committed: 2,
};

export class EPaxosAlgorithm implements ConsensusAlgorithm {
  readonly name = 'EPaxos';
  readonly description = 'Egalitarian Paxos — leaderless, optimal commit latency';
  private rng: () => number = Math.random;

  setRandomSource(rng: () => number): void {
    this.rng = rng;
  }

  getInitialState(nodeId: NodeId, config: ClusterConfig): NodeState {
    const allNodes = Array.from({ length: config.nodeCount }, (_, i) => `node_${i}`);
    const nodeIndex = parseInt(nodeId.split('_')[1]);

    return {
      id: nodeId,
      role: 'replica',
      status: 'alive',
      currentTerm: 0,
      votedFor: null,
      log: [],
      logBaseIndex: 0,
      logBaseTerm: 0,
      commitIndex: -1,
      lastApplied: -1,
      nextIndex: new Map(),
      matchIndex: new Map(),
      votesReceived: new Set(),
      meta: {
        peers: allNodes.filter(id => id !== nodeId),
        nodeIndex,
        instanceCounter: 0,
        instances: {} as Record<string, Instance>,
        commandQueue: [] as ClientRequest[],
        maxSeq: 0,
        activeInstance: null as string | null,  // currently proposing instance key
      },
    };
  }

  canAcceptClientRequest(node: NodeState): boolean {
    return node.role === 'replica' && node.status === 'alive';
  }

  getKnownLeader(_node: NodeState): NodeId | null { // eslint-disable-line @typescript-eslint/no-unused-vars
    return null; // no leader
  }

  onMessage(node: NodeState, msg: Message): Action[] {
    switch (msg.type) {
      case 'ep_preaccept': return this.handlePreAccept(node, msg);
      case 'ep_preaccept_ok': return this.handlePreAcceptOk(node, msg);
      case 'ep_accept': return this.handleAccept(node, msg);
      case 'ep_accept_ok': return this.handleAcceptOk(node, msg);
      case 'ep_commit': return this.handleCommit(node, msg);
      default: return [];
    }
  }

  onTimeout(node: NodeState, type: TimeoutType): Action[] {
    if (type === 'proposal') {
      // Proposal timeout — retry via slow path or re-propose
      const activeKey = node.meta.activeInstance as string | null;
      if (activeKey) {
        const instances = node.meta.instances as Record<string, Instance>;
        const inst = instances[activeKey];
        if (inst && inst.status === 'pre-accepted') {
          // Didn't get fast quorum — go to slow path
          return this.startSlowPath(node, activeKey, inst);
        }
      }
      // Try next command if queue non-empty
      if ((node.meta.commandQueue as ClientRequest[]).length > 0) {
        return this.proposeNext(node);
      }
    }
    return [];
  }

  onClientRequest(node: NodeState, request: ClientRequest): Action[] {
    (node.meta.commandQueue as ClientRequest[]).push(request);

    if (!node.meta.activeInstance) {
      return this.proposeNext(node);
    }
    return [];
  }

  onRecovery(node: NodeState, _config: ClusterConfig): Action[] { // eslint-disable-line @typescript-eslint/no-unused-vars
    node.role = 'replica';
    node.meta.activeInstance = null;
    return [];
  }

  // ---- Fast path: PreAccept ----

  private proposeNext(node: NodeState): Action[] {
    const queue = node.meta.commandQueue as ClientRequest[];
    if (queue.length === 0) {
      node.meta.activeInstance = null;
      return [];
    }

    const request = queue.shift()!;
    node.meta.instanceCounter = (node.meta.instanceCounter as number) + 1;
    const instanceKey = `${node.id}:${node.meta.instanceCounter}`;

    const seq = (node.meta.maxSeq as number) + 1;
    node.meta.maxSeq = seq;

    // Find dependencies: other replicas' uncommitted instances
    const deps = this.findDependencies(node, instanceKey);

    const instances = node.meta.instances as Record<string, Instance>;
    instances[instanceKey] = {
      entryId: request.entryId,
      requestId: request.requestId,
      command: request.command,
      status: 'pre-accepted',
      seq,
      deps,
      indexHint: null,
      ballot: 0,
      leaderNode: node.id,
      preAcceptOks: 1, // self
      acceptOks: 0,
      allDepsMatch: true,
    };

    node.meta.activeInstance = instanceKey;

    const peers = node.meta.peers as NodeId[];
    const actions: Action[] = [];

    // Send PreAccept to fast quorum (all peers for simplicity)
    for (const peer of peers) {
      actions.push({
        type: 'send_message',
        message: {
          type: 'ep_preaccept', from: node.id, to: peer,
          term: 0,
          payload: {
            instanceKey,
            command: request.command,
            seq,
            deps,
            requestId: request.requestId,
            entryId: request.entryId,
          },
        },
      });
    }

    // Timeout for slow path fallback
    actions.push({
      type: 'set_timeout',
      timeout: { type: 'proposal', duration: 300 + this.rng() * 200, nodeId: node.id },
    });

    return actions;
  }

  private handlePreAccept(node: NodeState, msg: Message): Action[] {
    const { instanceKey, command, seq, deps, requestId, entryId } = msg.payload as {
      instanceKey: string; command: string; seq: number; deps: string[]; requestId: string; entryId: string;
    };

    // Check for conflicts: find our own dependencies for this command
    const myDeps = this.findDependencies(node, instanceKey);
    let mySeq = seq;

    if (myDeps.length > 0) {
      const instances = node.meta.instances as Record<string, Instance>;
      const maxDepSeq = myDeps.reduce((maxSeq, depKey) => {
        const dep = instances[depKey];
        return dep ? Math.max(maxSeq, dep.seq) : maxSeq;
      }, node.meta.maxSeq as number);
      mySeq = Math.max(seq, maxDepSeq + 1);
    }
    node.meta.maxSeq = Math.max(node.meta.maxSeq as number, mySeq);

    const instance = this.upsertInstance(node, instanceKey, {
      entryId,
      requestId,
      command,
      status: 'pre-accepted',
      seq: mySeq,
      deps: myDeps,
      indexHint: null,
      ballot: 0,
      leaderNode: msg.from,
      preAcceptOks: 0,
      acceptOks: 0,
      allDepsMatch: true,
    });

    // Check if deps match what the leader proposed
    const depsMatch = instance.seq === seq && this.depsEqual(instance.deps, deps);

    return [{
      type: 'send_message',
      message: {
        type: 'ep_preaccept_ok', from: node.id, to: msg.from,
        term: 0,
        payload: { instanceKey, seq: instance.seq, deps: instance.deps, depsMatch },
      },
    }];
  }

  private handlePreAcceptOk(node: NodeState, msg: Message): Action[] {
    const { instanceKey, seq, deps, depsMatch } = msg.payload as {
      instanceKey: string; seq: number; deps: string[]; depsMatch: boolean;
    };

    const instances = node.meta.instances as Record<string, Instance>;
    const inst = instances[instanceKey];
    if (!inst || inst.status !== 'pre-accepted') return [];

    inst.preAcceptOks++;
    if (!depsMatch) {
      inst.allDepsMatch = false;
      // Merge deps and take max seq
      if (seq > inst.seq) inst.seq = seq;
      for (const d of deps) {
        if (!inst.deps.includes(d)) inst.deps.push(d);
      }
    }

    if (seq > (node.meta.maxSeq as number)) {
      node.meta.maxSeq = seq;
    }

    const peers = node.meta.peers as NodeId[];
    const fastQuorum = Math.floor((peers.length + 1) / 2) + 1; // floor(N/2) + 1

    if (inst.preAcceptOks >= fastQuorum) {
      if (inst.allDepsMatch) {
        // Fast path — commit directly!
        return this.commitInstance(node, instanceKey, inst);
      } else {
        // Slow path needed — start explicit Accept phase
        return this.startSlowPath(node, instanceKey, inst);
      }
    }
    return [];
  }

  // ---- Slow path: Accept ----

  private startSlowPath(node: NodeState, instanceKey: string, inst: Instance): Action[] {
    if (inst.status === 'committed') return [];
    inst.status = 'accepted';
    inst.acceptOks = 1; // self

    const peers = node.meta.peers as NodeId[];
    const actions: Action[] = [];

    for (const peer of peers) {
      actions.push({
        type: 'send_message',
        message: {
          type: 'ep_accept', from: node.id, to: peer,
          term: 0,
          payload: {
            instanceKey,
            command: inst.command,
            seq: inst.seq,
            deps: inst.deps,
            requestId: inst.requestId,
            entryId: inst.entryId,
          },
        },
      });
    }

    // New timeout for accept phase
    actions.push({
      type: 'set_timeout',
      timeout: { type: 'proposal', duration: 400 + this.rng() * 200, nodeId: node.id },
    });

    return actions;
  }

  private handleAccept(node: NodeState, msg: Message): Action[] {
    const { instanceKey, command, seq, deps, requestId, entryId } = msg.payload as {
      instanceKey: string; command: string; seq: number; deps: string[]; requestId: string; entryId: string;
    };

    this.upsertInstance(node, instanceKey, {
      entryId,
      requestId,
      command, status: 'accepted', seq, deps,
      indexHint: null,
      ballot: 0, leaderNode: msg.from,
      preAcceptOks: 0, acceptOks: 0, allDepsMatch: true,
    });

    if (seq > (node.meta.maxSeq as number)) {
      node.meta.maxSeq = seq;
    }

    return [{
      type: 'send_message',
      message: {
        type: 'ep_accept_ok', from: node.id, to: msg.from,
        term: 0,
        payload: { instanceKey },
      },
    }];
  }

  private handleAcceptOk(node: NodeState, msg: Message): Action[] {
    const { instanceKey } = msg.payload as { instanceKey: string };

    const instances = node.meta.instances as Record<string, Instance>;
    const inst = instances[instanceKey];
    if (!inst || inst.status !== 'accepted') return [];

    inst.acceptOks++;

    const peers = node.meta.peers as NodeId[];
    const majority = Math.floor((peers.length + 1) / 2) + 1;

    if (inst.acceptOks >= majority) {
      return this.commitInstance(node, instanceKey, inst);
    }
    return [];
  }

  // ---- Commit ----

  private commitInstance(node: NodeState, instanceKey: string, inst: Instance): Action[] {
    inst.status = 'committed';
    node.meta.activeInstance = null;

    const actions: Action[] = [];
    const materializedCount = this.materializeCommittedInstances(node);
    if (materializedCount > 0) {
      actions.push({ type: 'commit_entry' });
    }
    node.currentTerm = Math.max(node.currentTerm, inst.seq);

    // Broadcast commit to all
    const peers = node.meta.peers as NodeId[];
    for (const peer of peers) {
      actions.push({
        type: 'send_message',
        message: {
          type: 'ep_commit', from: node.id, to: peer,
          term: inst.seq,
          payload: {
            instanceKey, command: inst.command,
            seq: inst.seq, deps: inst.deps,
            index: this.findCommittedIndex(node, instanceKey, inst.entryId) ?? inst.indexHint,
            requestId: inst.requestId,
            entryId: inst.entryId,
          },
        },
      });
    }

    actions.push({
      type: 'cancel_timeout',
      timeout: { type: 'proposal', duration: 0, nodeId: node.id },
    });

    // Propose next command if queued
    if ((node.meta.commandQueue as ClientRequest[]).length > 0) {
      actions.push(...this.proposeNext(node));
    }

    return actions;
  }

  private handleCommit(node: NodeState, msg: Message): Action[] {
    const { instanceKey, command, seq, deps, requestId, entryId, index } = msg.payload as {
      instanceKey: string; command: string; seq: number; deps: string[]; requestId: string; entryId: string; index?: number;
    };

    const inst = this.upsertInstance(node, instanceKey, {
      entryId,
      requestId,
      command, status: 'committed', seq, deps,
      indexHint: index ?? null,
      ballot: 0, leaderNode: msg.from,
      preAcceptOks: 0, acceptOks: 0, allDepsMatch: true,
    });
    inst.status = 'committed';
    if (inst.indexHint === null && index !== undefined) {
      inst.indexHint = index;
    }

    if (seq > (node.meta.maxSeq as number)) {
      node.meta.maxSeq = seq;
    }

    this.materializeCommittedInstances(node);
    node.currentTerm = Math.max(node.currentTerm, seq);

    // Remove from queue
    const queue = node.meta.commandQueue as ClientRequest[];
    const idx = queue.findIndex(v => v.requestId === requestId);
    if (idx !== -1) queue.splice(idx, 1);

    return [];
  }

  // ---- Helpers ----

  private findDependencies(node: NodeState, excludeKey: string): string[] {
    const instances = node.meta.instances as Record<string, Instance>;
    const deps: string[] = [];
    for (const [key, inst] of Object.entries(instances)) {
      if (key === excludeKey) continue;
      // Simplified conflict: any uncommitted instance from a different replica
      if (inst.status !== 'committed' && inst.leaderNode !== node.id) {
        deps.push(key);
      }
    }
    return deps;
  }

  private depsEqual(a: string[], b: string[]): boolean {
    if (a.length !== b.length) return false;
    const setA = new Set(a);
    return b.every(d => setA.has(d));
  }

  private upsertInstance(node: NodeState, instanceKey: string, patch: Instance): Instance {
    const instances = node.meta.instances as Record<string, Instance>;
    const existing = instances[instanceKey];
    if (!existing) {
      instances[instanceKey] = {
        ...patch,
        deps: [...patch.deps],
      };
      return instances[instanceKey];
    }

    existing.entryId = patch.entryId;
    existing.requestId = patch.requestId;
    existing.command = patch.command;
    existing.seq = Math.max(existing.seq, patch.seq);
    existing.indexHint = existing.indexHint ?? patch.indexHint;
    existing.ballot = Math.max(existing.ballot, patch.ballot);
    existing.leaderNode = patch.leaderNode;
    existing.preAcceptOks = Math.max(existing.preAcceptOks, patch.preAcceptOks);
    existing.acceptOks = Math.max(existing.acceptOks, patch.acceptOks);
    existing.allDepsMatch = existing.allDepsMatch && patch.allDepsMatch;
    const mergedDeps = new Set([...existing.deps, ...patch.deps]);
    existing.deps = [...mergedDeps];
    if (STATUS_RANK[patch.status] > STATUS_RANK[existing.status]) {
      existing.status = patch.status;
    }
    return existing;
  }

  private upsertCommittedEntry(node: NodeState, entry: LogEntry): boolean {
    if (entry.index < node.logBaseIndex) return false;

    const existingIndex = node.log.findIndex(logEntry =>
      logEntry.entryId === entry.entryId
      || (entry.instanceKey !== undefined && logEntry.instanceKey === entry.instanceKey));

    if (existingIndex !== -1) {
      const existing = node.log[existingIndex];
      const mergedEntry = {
        ...existing,
        ...entry,
        committed: true,
      };
      if (existing.committed) {
        node.commitIndex = Math.max(node.commitIndex, mergedEntry.index);
        return false;
      }
      node.log[existingIndex] = mergedEntry;
      node.log.sort((a, b) => a.index - b.index);
      node.commitIndex = Math.max(node.commitIndex, mergedEntry.index);
      return true;
    }

    node.log.push(entry);
    node.log.sort((a, b) => a.index - b.index);
    node.commitIndex = Math.max(node.commitIndex, entry.index);
    return true;
  }

  private nextLogIndex(node: NodeState): number {
    const lastIndex = node.log.length > 0 ? node.log[node.log.length - 1].index : node.logBaseIndex - 1;
    return Math.max(node.logBaseIndex, node.commitIndex + 1, lastIndex + 1);
  }

  private findCommittedIndex(node: NodeState, instanceKey: string, entryId: string): number | null {
    const existing = node.log.find(entry =>
      entry.entryId === entryId
      || (entry.instanceKey !== undefined && entry.instanceKey === instanceKey));
    return existing?.index ?? null;
  }

  private materializeCommittedInstances(node: NodeState): number {
    const instances = node.meta.instances as Record<string, Instance>;
    let materializedCount = 0;
    let progressed = true;

    while (progressed) {
      progressed = false;
      const materialized = new Set(
        node.log
          .map(entry => entry.instanceKey)
          .filter((instanceKey): instanceKey is string => instanceKey !== undefined),
      );

      const batches = this.buildReadyExecutionBatches(instances, materialized);

      for (const batch of batches) {
        for (const [instanceKey, inst] of batch) {
          const nextIndex = this.nextLogIndex(node);
          const targetIndex = inst.indexHint !== null && inst.indexHint >= nextIndex
            ? inst.indexHint
            : nextIndex;
          const committed = this.upsertCommittedEntry(node, {
            entryId: inst.entryId,
            requestId: inst.requestId,
            instanceKey,
            term: inst.seq,
            index: targetIndex,
            command: inst.command,
            committed: true,
          });
          if (committed) {
            materializedCount++;
            progressed = true;
          }
        }
      }
    }

    return materializedCount;
  }

  private buildReadyExecutionBatches(
    instances: Record<string, Instance>,
    materialized: Set<string>,
  ): Array<Array<[string, Instance]>> {
    const committedPending = new Map(
      Object.entries(instances)
        .filter(([instanceKey, inst]) => inst.status === 'committed' && !materialized.has(instanceKey)),
    );

    if (committedPending.size === 0) return [];

    const edges = new Map<string, string[]>();
    const reverseEdges = new Map<string, string[]>();

    for (const [instanceKey, inst] of committedPending) {
      const localDeps = inst.deps.filter(dep => committedPending.has(dep));
      edges.set(instanceKey, localDeps);
      if (!reverseEdges.has(instanceKey)) reverseEdges.set(instanceKey, []);
      for (const dep of localDeps) {
        if (!reverseEdges.has(dep)) reverseEdges.set(dep, []);
        reverseEdges.get(dep)!.push(instanceKey);
      }
    }

    const order: string[] = [];
    const visited = new Set<string>();
    const dfs = (key: string): void => {
      if (visited.has(key)) return;
      visited.add(key);
      for (const dep of edges.get(key) ?? []) dfs(dep);
      order.push(key);
    };

    for (const key of committedPending.keys()) dfs(key);

    const assigned = new Set<string>();
    const batches: Array<Array<[string, Instance]>> = [];

    for (let i = order.length - 1; i >= 0; i--) {
      const root = order[i];
      if (assigned.has(root)) continue;

      const componentKeys: string[] = [];
      const stack = [root];
      assigned.add(root);

      while (stack.length > 0) {
        const key = stack.pop()!;
        componentKeys.push(key);
        for (const dep of reverseEdges.get(key) ?? []) {
          if (!assigned.has(dep)) {
            assigned.add(dep);
            stack.push(dep);
          }
        }
      }

      const component = new Set(componentKeys);
      const hasBlockedExternalDeps = componentKeys.some(key =>
        (committedPending.get(key)?.deps ?? []).some(dep => !component.has(dep) && !materialized.has(dep)));

      if (hasBlockedExternalDeps) continue;

      const batch = componentKeys
        .map(key => [key, committedPending.get(key)!] as [string, Instance])
        .sort(([leftKey, left], [rightKey, right]) => left.seq - right.seq || leftKey.localeCompare(rightKey));
      batches.push(batch);
    }

    return batches.sort((leftBatch, rightBatch) => {
      const [leftKey, leftInst] = leftBatch[0];
      const [rightKey, rightInst] = rightBatch[0];
      return leftInst.seq - rightInst.seq || leftKey.localeCompare(rightKey);
    });
  }
}
