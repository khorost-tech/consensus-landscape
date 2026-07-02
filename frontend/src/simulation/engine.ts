import {
  SimEvent, NodeState, NodeId, Action, ClusterConfig,
  Message, SimulationMetrics, EventType, TimeoutType,
  LiveStats, ClientState, LogEntry,
} from './types';
import { ConsensusAlgorithm } from './algorithms/interface';
import { NetworkModel } from './network';
import {
  LATENCY_WINDOW_SIZE, CLIENT_RETRY_DELAY_BASE, CLIENT_RETRY_DELAY_JITTER, DEAD_NODE_RETRY_DELAY,
  METRICS_HISTORY_LIMIT, NODE_LOG_LIMIT, MESSAGE_EDGE_DELIVERY_FRACTION,
} from './constants';

export interface ActiveMessage {
  message: Message;
  sendTime: number;
  /** Time when message is logically delivered (edge of node) — triggers algorithm processing */
  deliverTime: number;
  /** Time when visual animation ends (center of node) — used for cleanup */
  arriveTime: number;
  dropped: boolean;
}

/** Tracks a client request through the system */
interface PendingRequest {
  time: number;
  requestId: string;
  entryId: string;
  command: string;
  clientId: string;
  targetNode: NodeId;
  retries: number;
  /** Visual state: pending (client→node), replicating (node→followers), committed (response→client) */
  connectionState: 'pending' | 'replicating' | 'committed';
}

/** Timeout with actual scheduling info for accurate progress */
interface TimeoutInfo {
  event: SimEvent;
  startTime: number;
  duration: number;
}

/** Client connection state exposed for visualization */
export interface ClientConnection {
  clientId: string;
  targetNode: NodeId;
  command: string;
  state: 'pending' | 'replicating' | 'committed';
}

/** Min-heap priority queue for SimEvents (sorted by time) */
class EventHeap {
  private heap: SimEvent[] = [];
  private cancelledIds: Set<string> = new Set();

  get length(): number { return this.heap.length; }

  push(event: SimEvent): void {
    this.heap.push(event);
    this._siftUp(this.heap.length - 1);
  }

  peek(): SimEvent | undefined {
    this._skipCancelled();
    return this.heap[0];
  }

  pop(): SimEvent | undefined {
    this._skipCancelled();
    if (this.heap.length === 0) return undefined;
    const top = this.heap[0];
    const last = this.heap.pop()!;
    if (this.heap.length > 0) {
      this.heap[0] = last;
      this._siftDown(0);
    }
    return top;
  }

  cancel(eventId: string): void {
    this.cancelledIds.add(eventId);
  }

  /** Remove cancelled entries from heap and clear cancelledIds set */
  purgeCancelled(): void {
    if (this.cancelledIds.size === 0) return;
    this.heap = this.heap.filter(e => !this.cancelledIds.has(e.id));
    this.cancelledIds.clear();
    this._rebuild();
  }

  /** Remove cancelled events from pending messages */
  filterMessages(predicate: (e: SimEvent) => boolean): void {
    // Used rarely (node_failure) — OK to be O(n)
    this.heap = this.heap.filter(predicate);
    this._rebuild();
  }

  private _skipCancelled(): void {
    while (this.heap.length > 0 && this.cancelledIds.has(this.heap[0].id)) {
      this.cancelledIds.delete(this.heap[0].id);
      const last = this.heap.pop()!;
      if (this.heap.length > 0) {
        this.heap[0] = last;
        this._siftDown(0);
      }
    }
  }

  private _siftUp(i: number): void {
    const h = this.heap;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (h[i].time >= h[parent].time) break;
      [h[i], h[parent]] = [h[parent], h[i]];
      i = parent;
    }
  }

  private _siftDown(i: number): void {
    const h = this.heap;
    const n = h.length;
    while (true) {
      let smallest = i;
      const left = 2 * i + 1;
      const right = 2 * i + 2;
      if (left < n && h[left].time < h[smallest].time) smallest = left;
      if (right < n && h[right].time < h[smallest].time) smallest = right;
      if (smallest === i) break;
      [h[i], h[smallest]] = [h[smallest], h[i]];
      i = smallest;
    }
  }

  private _rebuild(): void {
    for (let i = (this.heap.length >> 1) - 1; i >= 0; i--) {
      this._siftDown(i);
    }
  }
}

export class SimulationEngine {
  private time = 0;
  private nodes: Map<NodeId, NodeState> = new Map();
  private eventQueue = new EventHeap();
  private activeTimeouts: Map<string, TimeoutInfo> = new Map();
  private network: NetworkModel;
  private algorithm: ConsensusAlgorithm;
  private config: ClusterConfig;
  private metrics: SimulationMetrics;
  private activeMessages: ActiveMessage[] = [];
  private rng: () => number;
  private nextEventId = 0;
  private nextMsgId = 0;
  private nextRequestId = 0;

  private pendingClientRequests: Map<string, PendingRequest> = new Map();
  private liveStats: LiveStats;
  private clients: ClientState[];
  private latencyWindow: number[] = [];
  private prevHasQuorum = true;
  private prevHasLeader = true;
  private noQuorumStart = -1;
  private stepCount = 0;
  private electingStart = -1;
  private lastLeaderLostTime = 0; // when the last leader was lost (for re-election timing)

  constructor(algorithm: ConsensusAlgorithm, config: ClusterConfig, seed?: number) {
    this.algorithm = algorithm;
    this.config = config;
    this.network = new NetworkModel(config.networkConfig);
    this.rng = createSeededRng(seed ?? Date.now());
    this.algorithm.setRandomSource(this.rng);
    this.metrics = {
      commitTimestamps: [],
      commitLatencies: [],
      leaderChangeTimestamps: [],
      nodeEvents: [],
      conflictTimestamps: [],
      statusZones: [],
    };
    this.liveStats = {
      totalMessages: 0,
      droppedMessages: 0,
      totalCommits: 0,
      leaderChanges: 0,
      avgLatency: 0,
      currentLeader: null,
      currentTerm: 0,
      electionTime: null,
      rejectedRequests: 0,
      nackCount: 0,
      quorumSize: Math.floor(config.nodeCount / 2) + 1,
    };

    const initialClients = config.clientCount ?? 2;
    this.clients = [];
    for (let i = 0; i < initialClients; i++) {
      this.clients.push({ id: `client_${i}`, pendingCommand: null, targetNode: null, completedCommands: 0, lastLatency: null });
    }

    this.initializeNodes();
  }

  private initializeNodes(): void {
    for (let i = 0; i < this.config.nodeCount; i++) {
      const nodeId = `node_${i}`;
      const state = this.algorithm.getInitialState(nodeId, this.config);
      this.nodes.set(nodeId, state);

      // Schedule initial election timeout for algorithms that use elections.
      // Paxos and EPaxos schedule timeouts on demand.
      if (this.algorithm.name !== 'Paxos' && this.algorithm.name !== 'EPaxos') {
        const timeout = this.randomElectionTimeout();
        this.scheduleTimeout(nodeId, 'election', timeout);
      }
    }
  }

  private randomElectionTimeout(): number {
    const { electionTimeoutMin, electionTimeoutMax } = this.config;
    return electionTimeoutMin + this.rng() * (electionTimeoutMax - electionTimeoutMin);
  }

  private generateEventId(): string {
    return `evt_${this.nextEventId++}`;
  }

  private generateMsgId(): string {
    return `msg_${this.nextMsgId++}`;
  }

  private generateRequestId(): string {
    return `req_${this.nextRequestId++}`;
  }

  private scheduleTimeout(nodeId: NodeId, type: TimeoutType, duration: number): void {
    const key = `${nodeId}:${type}`;
    const existing = this.activeTimeouts.get(key);
    if (existing) {
      this.eventQueue.cancel(existing.event.id); // O(1) lazy cancel
    }
      const event: SimEvent = {
      id: this.generateEventId(),
      time: this.time + duration,
      type: 'timeout',
      target: nodeId,
      payload: { timeoutType: type },
    };
    this.activeTimeouts.set(key, { event, startTime: this.time, duration });
    this.eventQueue.push(event); // O(log n)
  }

  private cancelTimeout(nodeId: NodeId, type: TimeoutType): void {
    const key = `${nodeId}:${type}`;
    const existing = this.activeTimeouts.get(key);
    if (existing) {
      this.eventQueue.cancel(existing.event.id); // O(1) lazy cancel
      this.activeTimeouts.delete(key);
    }
  }

  private insertEvent(event: SimEvent): void {
    this.eventQueue.push(event); // O(log n)
  }

  private processActions(nodeId: NodeId, actions: Action[]): void {
    for (const action of actions) {
      switch (action.type) {
        case 'send_message': {
          if (!action.message) break;
          const msg: Message = { ...action.message, id: this.generateMsgId() };

          this.liveStats.totalMessages++;
          if (msg.type === 'nack') {
            this.liveStats.nackCount++;
            this.metrics.conflictTimestamps.push(this.time);
          }

          // Intercept client_response with redirect
          if (msg.type === 'client_response' && msg.payload.redirect) {
            this.handleClientRedirect(nodeId, msg);
            break;
          }

          const delay = this.network.getDelay(msg.from, msg.to, this.rng);
          const dropped = this.network.isDropped(msg.from, msg.to, this.rng);
          const deliverTime = this.time + delay * MESSAGE_EDGE_DELIVERY_FRACTION;

          const activeMsg: ActiveMessage = {
            message: msg,
            sendTime: this.time,
            deliverTime,
            arriveTime: this.time + delay,
            dropped,
          };
          this.activeMessages.push(activeMsg);

          if (!dropped) {
            this.insertEvent({
              id: this.generateEventId(),
              time: deliverTime,
              type: 'message_arrive',
              target: msg.to,
              payload: { message: msg },
            });
          } else {
            this.liveStats.droppedMessages++;
          }
          break;
        }
        case 'set_timeout': {
          if (action.timeout) this.scheduleTimeout(action.timeout.nodeId, action.timeout.type, action.timeout.duration);
          break;
        }
        case 'cancel_timeout': {
          if (action.timeout) this.cancelTimeout(action.timeout.nodeId, action.timeout.type);
          break;
        }
        case 'commit_entry': {
          this.handleCommitEntry(nodeId);
          break;
        }
        case 'apply_entry': {
          this.applyCommittedEntries(nodeId);
          break;
        }
      }
    }
  }

  private handleCommitEntry(nodeId: NodeId): void {
    const node = this.nodes.get(nodeId);
    if (!node) return;

    node.meta.lastCommitTime = this.time;
    for (const entry of node.log) {
      if (!entry.committed) continue;

      const req = this.pendingClientRequests.get(entry.requestId);
      if (!req) continue;

      req.connectionState = 'committed';
      const latency = this.time - req.time;
      this.metrics.commitTimestamps.push(this.time);
      this.metrics.commitLatencies.push(latency);
      this.liveStats.totalCommits++;
      if (this.liveStats.electionTime === null) {
        this.liveStats.electionTime = this.time;
      }

      this.latencyWindow.push(latency);
      if (this.latencyWindow.length > LATENCY_WINDOW_SIZE) this.latencyWindow.shift();
      this.liveStats.avgLatency = this.latencyWindow.reduce((a, b) => a + b, 0) / this.latencyWindow.length;

      const client = this.clients.find(c => c.id === req.clientId);
      if (client) {
        client.pendingCommand = null;
        client.completedCommands++;
        client.lastLatency = latency;

        const respDelay = this.network.getDelay(nodeId, nodeId, this.rng);
        const respMsg: Message = {
          id: this.generateMsgId(),
          type: 'client_response',
          from: nodeId,
          to: req.clientId,
          term: 0,
          payload: { command: entry.command, requestId: entry.requestId, entryId: entry.entryId },
        };
        this.activeMessages.push({
          message: respMsg,
          sendTime: this.time,
          deliverTime: this.time + respDelay,
          arriveTime: this.time + respDelay,
          dropped: false,
        });
      }

      this.pendingClientRequests.delete(entry.requestId);
    }
  }

  private applyCommittedEntries(nodeId: NodeId): void {
    const node = this.nodes.get(nodeId);
    if (!node || node.status !== 'alive') return;

    const appliedEntries = node.log
      .filter(entry => entry.committed && entry.index > node.lastApplied)
      .sort((left, right) => left.index - right.index);

    if (appliedEntries.length === 0) return;

    node.lastApplied = appliedEntries[appliedEntries.length - 1].index;
    node.meta.lastApplyTime = this.time;
  }

  /** Handle redirect: client got rejected, needs to find the real leader */
  private handleClientRedirect(rejectedBy: NodeId, msg: Message): void {
    const leaderHint = msg.payload.leaderHint as NodeId | null;
    this.liveStats.rejectedRequests++;

    // Find which pending request was for this node
    for (const [requestId, req] of this.pendingClientRequests) {
      if (req.targetNode === rejectedBy) {
        req.retries++;

        // Try the hinted leader, or a random alive node
        let nextTarget = leaderHint;
        if (!nextTarget || this.nodes.get(nextTarget)?.status !== 'alive') {
          // Pick random alive non-rejected node
          const alive = Array.from(this.nodes.entries())
            .filter(([id, n]) => n.status === 'alive' && id !== rejectedBy);
          if (alive.length > 0) {
            nextTarget = alive[Math.floor(this.rng() * alive.length)][0];
          } else {
            nextTarget = rejectedBy; // no other option
          }
        }

        req.targetNode = nextTarget;

        // Update client visual
        const client = this.clients.find(c => c.id === req.clientId);
        if (client) {
          client.targetNode = nextTarget;
        }

        // Create a visible "redirect" message from old node to new
        const redirectMsg: Message = {
          id: this.generateMsgId(),
          type: 'client_request',
          from: rejectedBy,
          to: nextTarget,
          term: 0,
          payload: { redirect: true, originalClient: req.clientId },
        };

        // If no leader hint — election in progress, add extra wait before retry
        const networkDelay = this.network.getDelay(rejectedBy, nextTarget, this.rng);
        const retryDelay = leaderHint ? networkDelay : networkDelay + CLIENT_RETRY_DELAY_BASE + this.rng() * CLIENT_RETRY_DELAY_JITTER;

        const redirectDeliverTime = this.time + retryDelay * MESSAGE_EDGE_DELIVERY_FRACTION;
        this.activeMessages.push({
          message: redirectMsg,
          sendTime: this.time,
          deliverTime: redirectDeliverTime,
          arriveTime: this.time + retryDelay,
          dropped: false,
        });

        // Re-inject the client request to the new target after delay
        this.insertEvent({
          id: this.generateEventId(),
          time: redirectDeliverTime,
          type: 'client_request',
          target: nextTarget,
          payload: { command: req.command, clientId: req.clientId, requestId, entryId: req.entryId },
        });

        break; // handle one redirect at a time
      }
    }
  }

  step(): SimEvent | null {
    const event = this.eventQueue.pop(); // O(log n) heap pop
    if (!event) return null;
    this.time = event.time;

    const node = this.nodes.get(event.target);
    if (!node) return event;

    if (
      node.status === 'dead'
      && event.type !== 'node_recovery'
      && event.type !== 'network_partition'
      && event.type !== 'heal_partition'
    ) {
      // Node is dead — if this was a client request, trigger redirect
      if (event.type === 'client_request' && event.payload.command) {
        const clientId = event.payload.clientId ?? 'client_0';
        const requestId = event.payload.requestId;
        const req = requestId ? this.pendingClientRequests.get(requestId) : undefined;
        if (req) {
          // Simulate timeout → client retries to another node
          const alive = Array.from(this.nodes.entries())
            .filter(([id, n]) => n.status === 'alive' && id !== event.target);
          if (alive.length > 0) {
            const nextTarget = alive[Math.floor(this.rng() * alive.length)][0];
            req.targetNode = nextTarget;
            req.retries++;

            const client = this.clients.find(c => c.id === clientId);
            if (client) client.targetNode = nextTarget;

            // Retry after a short timeout (simulating client timeout)
            this.insertEvent({
              id: this.generateEventId(),
              time: this.time + DEAD_NODE_RETRY_DELAY,
              type: 'client_request',
              target: nextTarget,
              payload: { command: event.payload.command, clientId, requestId: req.requestId, entryId: req.entryId },
            });
          }
        }
      }
      return event;
    }

    const prevRole = node.role;
    let actions: Action[] = [];

    switch (event.type) {
      case 'message_arrive': {
        if (event.payload.message) {
          this.activeMessages = this.activeMessages.filter(
            am => am.message.id !== event.payload.message!.id
          );
          actions = this.algorithm.onMessage(node, event.payload.message);
        }
        break;
      }
      case 'timeout': {
        if (event.payload.timeoutType) {
          const key = `${event.target}:${event.payload.timeoutType}`;
          this.activeTimeouts.delete(key);
          actions = this.algorithm.onTimeout(node, event.payload.timeoutType);
        }
        break;
      }
      case 'client_request': {
        if (event.payload.command) {
          const clientId = event.payload.clientId ?? 'client_0';
          const requestId = event.payload.requestId ?? this.generateRequestId();
          const entryId = event.payload.entryId ?? requestId;

          if (!this.pendingClientRequests.has(requestId)) {
            this.pendingClientRequests.set(requestId, {
              time: this.time,
              requestId,
              entryId,
              command: event.payload.command,
              clientId,
              targetNode: event.target,
              retries: 0,
              connectionState: 'pending',
            });
          }

          const client = this.clients.find(c => c.id === clientId);
          if (client) {
            client.pendingCommand = event.payload.command;
            client.targetNode = event.target;
          }

          actions = this.algorithm.onClientRequest(node, {
            requestId,
            entryId,
            command: event.payload.command,
          });

          // If node accepted the request (returns send_message), transition to replicating
          const accepted = actions.some(a => a.type === 'send_message');
          if (accepted) {
            const req = this.pendingClientRequests.get(requestId);
            if (req) req.connectionState = 'replicating';
          }
        }
        break;
      }
      case 'node_failure': {
        node.status = 'dead';
        this.metrics.nodeEvents.push({ time: this.time, type: 'failure', nodeId: event.target });
        this.cancelTimeout(event.target, 'election');
        this.cancelTimeout(event.target, 'heartbeat');
        const toRemove = new Set<string>();
        this.activeMessages = this.activeMessages.filter(am => {
          if (am.message.to === event.target || am.message.from === event.target) {
            toRemove.add(am.message.id);
            return false;
          }
          return true;
        });
        this.eventQueue.filterMessages(
          e => !(e.type === 'message_arrive' && e.payload.message &&
            toRemove.has(e.payload.message.id))
        );
        break;
      }
      case 'node_recovery': {
        node.status = 'alive';
        this.metrics.nodeEvents.push({ time: this.time, type: 'recovery', nodeId: event.target });
        actions = this.algorithm.onRecovery(node, this.config);

        // State transfer — send committed entries the node missed (Raft handles this via AppendEntries)
        if (this.algorithm.name !== 'Raft') {
          this.scheduleCatchUp(event.target);
        }
        break;
      }
      case 'network_partition': {
        this.setPartitions(event.payload.partitions ?? []);
        break;
      }
      case 'heal_partition': {
        this.setPartitions([]);
        break;
      }
    }

    this.processActions(event.target, actions);
    this.processActions(event.target, [{ type: 'apply_entry' }]);

    // Track leader changes and current state
    if (prevRole !== 'leader' && prevRole !== 'leading' && (node.role === 'leader' || node.role === 'leading')) {
      this.liveStats.leaderChanges++;
      this.metrics.leaderChangeTimestamps.push(this.time);
      // Record election duration (from leader loss or simulation start)
      this.liveStats.electionTime = this.time - this.lastLeaderLostTime;
    }

    let maxTerm = 0;
    let currentLeader: NodeId | null = null;
    for (const [, n] of this.nodes) {
      if (n.currentTerm > maxTerm) maxTerm = n.currentTerm;
      if ((n.role === 'leader' || n.role === 'leading') && n.status === 'alive') currentLeader = n.id;
    }
    this.liveStats.currentTerm = maxTerm;
    this.liveStats.currentLeader = currentLeader;

    // Track cluster status zones
    const { hasQuorum } = this.getClusterStatus();

    // No-quorum zone
    if (!hasQuorum && this.prevHasQuorum) {
      this.noQuorumStart = this.time;
    } else if (hasQuorum && !this.prevHasQuorum && this.noQuorumStart >= 0) {
      this.metrics.statusZones.push({ start: this.noQuorumStart, end: this.time, type: 'no_quorum' });
      this.noQuorumStart = -1;
    }
    this.prevHasQuorum = hasQuorum;

    // Electing zone (no leader but have quorum)
    const hasLeaderNow = !!currentLeader;
    if (!hasLeaderNow && this.prevHasLeader) {
      this.lastLeaderLostTime = this.time; // record when leader was lost
      if (hasQuorum) this.electingStart = this.time;
    } else if (hasLeaderNow && !this.prevHasLeader && this.electingStart >= 0) {
      this.metrics.statusZones.push({ start: this.electingStart, end: this.time, type: 'electing' });
      this.electingStart = -1;
    }
    this.prevHasLeader = hasLeaderNow;

    // Periodic garbage collection every 200 steps
    if (++this.stepCount % 200 === 0) {
      this.trimHistory();
    }

    return event;
  }

  setPartitions(partitions: NodeId[][]): void {
    this.config = {
      ...this.config,
      networkConfig: {
        ...this.config.networkConfig,
        partitions: partitions.map(group => [...group]),
      },
    };
    this.network.updateConfig(this.config.networkConfig);
  }

  clearPartitions(): void {
    this.setPartitions([]);
  }

  getPartitions(): NodeId[][] {
    return this.config.networkConfig.partitions.map(group => [...group]);
  }

  /** Trim accumulated history to bound memory during long simulations */
  private trimHistory(): void {
    const ml = METRICS_HISTORY_LIMIT;

    // Trim metrics arrays (keep most recent entries)
    if (this.metrics.commitTimestamps.length > ml) {
      const excess = this.metrics.commitTimestamps.length - ml;
      this.metrics.commitTimestamps.splice(0, excess);
      this.metrics.commitLatencies.splice(0, excess);
    }
    if (this.metrics.conflictTimestamps.length > ml) {
      this.metrics.conflictTimestamps.splice(0, this.metrics.conflictTimestamps.length - ml);
    }
    if (this.metrics.leaderChangeTimestamps.length > ml) {
      this.metrics.leaderChangeTimestamps.splice(0, this.metrics.leaderChangeTimestamps.length - ml);
    }
    if (this.metrics.nodeEvents.length > ml) {
      this.metrics.nodeEvents.splice(0, this.metrics.nodeEvents.length - ml);
    }
    if (this.metrics.statusZones.length > ml) {
      this.metrics.statusZones.splice(0, this.metrics.statusZones.length - ml);
    }

    // Trim node logs (keep last NODE_LOG_LIMIT entries)
    for (const [, node] of this.nodes) {
      if (node.log.length > NODE_LOG_LIMIT) {
        const excess = node.log.length - NODE_LOG_LIMIT;
        const lastTrimmedEntry = node.log[excess - 1];
        node.logBaseIndex = lastTrimmedEntry.index + 1;
        node.logBaseTerm = lastTrimmedEntry.term;
        node.log.splice(0, excess);
      }
    }

    // Purge stale cancelled IDs from heap
    this.eventQueue.purgeCancelled();

    // Clean up old active messages that should have arrived already
    this.activeMessages = this.activeMessages.filter(
      am => am.arriveTime >= this.time - 500
    );
  }

  runUntil(targetTime: number): SimEvent[] {
    const events: SimEvent[] = [];
    let next = this.eventQueue.peek();
    while (next && next.time <= targetTime) {
      const event = this.step();
      if (event) events.push(event);
      next = this.eventQueue.peek();
    }
    this.time = targetTime;
    return events;
  }

  /** Recovery catch-up: find best alive peer and send committed entries the node missed */
  private scheduleCatchUp(recoveredNodeId: NodeId): void {
    const recoveredNode = this.nodes.get(recoveredNodeId);
    if (!recoveredNode) return;

    // Find alive peer with the most committed log entries
    let bestPeer: NodeState | null = null;
    let bestCommitCount = 0;
    for (const [id, n] of this.nodes) {
      if (id === recoveredNodeId || n.status !== 'alive') continue;
      const committed = n.commitIndex + 1;
      if (committed > bestCommitCount) {
        bestCommitCount = committed;
        bestPeer = n;
      }
    }

    if (!bestPeer || bestCommitCount === 0) return;

    // Determine which commands the recovered node is missing
    const knownEntryIds = new Set(recoveredNode.log.filter(e => e.committed).map(e => e.entryId));
    const missingEntries = bestPeer.log.filter(e => e.committed && !knownEntryIds.has(e.entryId));

    // Schedule catch-up messages using algorithm-appropriate message type
    for (const entry of missingEntries) {
      const delay = this.network.getDelay(bestPeer.id, recoveredNodeId, this.rng);
      const msg = this.buildCatchUpMessage(bestPeer.id, recoveredNodeId, entry);
      const deliverTime = this.time + delay * MESSAGE_EDGE_DELIVERY_FRACTION;
      this.activeMessages.push({
        message: msg,
        sendTime: this.time,
        deliverTime,
        arriveTime: this.time + delay,
        dropped: false,
      });
      this.insertEvent({
        id: this.generateEventId(),
        time: deliverTime,
        type: 'message_arrive',
        target: recoveredNodeId,
        payload: { message: msg },
      });
    }
  }

  /** Build a catch-up message appropriate for the current algorithm */
  private buildCatchUpMessage(from: NodeId, to: NodeId, entry: LogEntry): Message {
    const base = { id: this.generateMsgId(), from, to, term: entry.term };
    switch (this.algorithm.name) {
      case 'EPaxos':
        return {
          ...base, type: 'ep_commit',
          payload: {
            instanceKey: entry.instanceKey ?? `${from}:recovery_${entry.index}`,
            command: entry.command, seq: entry.term, deps: [], index: entry.index,
            requestId: entry.requestId, entryId: entry.entryId,
          },
        };
      case 'Zab':
        return {
          ...base, type: 'zab_sync',
          payload: {
            entry: {
              entryId: entry.entryId,
              requestId: entry.requestId,
              zxid: entry.zxid,
              term: entry.term,
              index: entry.index,
              command: entry.command,
              committed: true,
            },
          },
        };
      default: // Paxos, Multi-Paxos, Raft
        return {
          ...base, type: 'learn',
          payload: {
            slot: entry.index,
            value: { requestId: entry.requestId, entryId: entry.entryId, command: entry.command },
            proposalNumber: entry.term,
            commitIndex: entry.index,
          },
        };
    }
  }

  injectEvent(type: EventType, target: NodeId, time: number, payload: SimEvent['payload'] = {}): string {
    const id = this.generateEventId();
    this.insertEvent({ id, time, type, target, payload });
    return id;
  }

  cancelEvent(eventId: string): void {
    this.eventQueue.cancel(eventId);
  }

  /** Submit client request — client remembers last known leader */
  submitClientRequest(command: string, clientId?: string, targetNode?: NodeId): void {
    const requestId = this.generateRequestId();
    const entryId = requestId;
    const cid = clientId ?? 'client_0';
    const client = this.clients.find(c => c.id === cid);

    // Client tries: explicit target → last successful target → known leader → random alive node
    let target = targetNode;
    if (!target && client?.targetNode) {
      // Try last known target first
      const lastTarget = this.nodes.get(client.targetNode);
      if (lastTarget && lastTarget.status === 'alive') {
        target = client.targetNode;
      }
    }
    if (!target) {
      // Find a leader
      for (const [id, node] of this.nodes) {
        if (node.role === 'leader' && node.status === 'alive') {
          target = id;
          break;
        }
      }
    }
    if (!target) {
      // Random alive node
      const alive = Array.from(this.nodes.entries()).filter(([, n]) => n.status === 'alive');
      if (alive.length > 0) {
        target = alive[Math.floor(this.rng() * alive.length)][0];
      } else {
        target = Array.from(this.nodes.keys())[0];
      }
    }

    // Create visible message from client to node
    const delay = this.network.getDelay(target, target, this.rng); // approximate
    const clientMsg: Message = {
      id: this.generateMsgId(),
      type: 'client_request',
      from: cid,
      to: target,
      term: 0,
      payload: { command, requestId, entryId },
    };
    const clientDeliverTime = this.time + delay * MESSAGE_EDGE_DELIVERY_FRACTION;
    this.activeMessages.push({
      message: clientMsg,
      sendTime: this.time,
      deliverTime: clientDeliverTime,
      arriveTime: this.time + delay,
      dropped: false,
    });

    this.injectEvent('client_request', target, clientDeliverTime, { command, clientId: cid, requestId, entryId });
  }

  addClient(): string {
    const id = `client_${this.clients.length}`;
    this.clients.push({ id, pendingCommand: null, targetNode: null, completedCommands: 0, lastLatency: null });
    return id;
  }

  removeClient(): string | null {
    if (this.clients.length <= 1) return null;
    const removed = this.clients.pop()!;
    // Cancel any pending requests for this client
    for (const [requestId, req] of this.pendingClientRequests) {
      if (req.clientId === removed.id) {
        this.pendingClientRequests.delete(requestId);
      }
    }
    return removed.id;
  }

  getClientCount(): number { return this.clients.length; }

  /** Get timeout progress for each node: { nodeId → { type, progress 0..1 } } */
  getTimeoutProgress(): Map<NodeId, { type: TimeoutType; progress: number }[]> {
    const result = new Map<NodeId, { type: TimeoutType; progress: number }[]>();
    for (const [key, info] of this.activeTimeouts) {
      const [nodeId, typeStr] = key.split(':');
      const type = typeStr as TimeoutType;
      const elapsed = this.time - info.startTime;
      const progress = Math.max(0, Math.min(1, elapsed / info.duration));
      const list = result.get(nodeId) ?? [];
      list.push({ type, progress });
      result.set(nodeId, list);
    }
    return result;
  }

  /** Get active client connections for visualization */
  getClientConnections(): ClientConnection[] {
    const connections: ClientConnection[] = [];
    for (const [, req] of this.pendingClientRequests) {
      connections.push({
        clientId: req.clientId,
        targetNode: req.targetNode,
        command: req.command,
        state: req.connectionState,
      });
    }
    return connections;
  }

  /** Check cluster health: has quorum, is electing */
  getClusterStatus(): { hasQuorum: boolean; isElecting: boolean } {
    let aliveCount = 0;
    let hasLeader = false;
    let hasCandidate = false;
    for (const [, n] of this.nodes) {
      if (n.status === 'alive') {
        aliveCount++;
        if (n.role === 'leader' || n.role === 'leading') hasLeader = true;
        if (n.role === 'candidate' || n.role === 'looking') hasCandidate = true;
      }
    }
    const quorumSize = Math.floor(this.config.nodeCount / 2) + 1;
    return {
      hasQuorum: aliveCount >= quorumSize,
      isElecting: !hasLeader && (hasCandidate || aliveCount >= quorumSize),
    };
  }

  getTime(): number { return this.time; }
  getNodes(): Map<NodeId, NodeState> { return this.nodes; }
  getEventQueueSize(): number { return this.eventQueue.length; }
  getMetrics(): SimulationMetrics {
    // Include currently open zones up to current time
    const zones = [...this.metrics.statusZones];
    if (this.noQuorumStart >= 0) {
      zones.push({ start: this.noQuorumStart, end: this.time, type: 'no_quorum' });
    }
    if (this.electingStart >= 0) {
      zones.push({ start: this.electingStart, end: this.time, type: 'electing' });
    }
    return { ...this.metrics, statusZones: zones };
  }
  getActiveMessages(): ActiveMessage[] { return this.activeMessages; }
  getNodeIds(): NodeId[] { return Array.from(this.nodes.keys()); }
  getLiveStats(): LiveStats { return { ...this.liveStats }; }
  getClients(): ClientState[] { return this.clients.map(c => ({ ...c })); }
}

function createSeededRng(seed: number): () => number {
  let s = seed | 0;
  return () => {
    s = (s + 0x6D2B79F5) | 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
