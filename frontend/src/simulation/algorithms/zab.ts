import { ConsensusAlgorithm } from './interface';
import {
  NodeState, NodeId, Message, Action, ClusterConfig, TimeoutType, LogEntry, ClientRequest,
} from '../types';

/**
 * Zab (ZooKeeper Atomic Broadcast) — leader-based consensus with total ordering.
 *
 * Three phases:
 * 1. Discovery/Election — elect leader with highest zxid (epoch, counter)
 * 2. Synchronization — leader brings followers up to date
 * 3. Broadcast — leader proposes, followers ack, leader commits
 *
 * Key differences from Raft visible in simulation:
 * - Leader elected by highest zxid, not random timeout
 * - Explicit synchronization phase before broadcast
 * - Three-step commit: proposal → ack → commit (vs Raft's two-step)
 * - Epoch-based versioning (visible in term display)
 */
export class ZabAlgorithm implements ConsensusAlgorithm {
  readonly name = 'Zab';
  readonly description = 'ZooKeeper Atomic Broadcast — leader-based total order with epochs';
  private rng: () => number = Math.random;

  setRandomSource(rng: () => number): void {
    this.rng = rng;
  }

  getInitialState(nodeId: NodeId, config: ClusterConfig): NodeState {
    const allNodes = Array.from({ length: config.nodeCount }, (_, i) => `node_${i}`);
    const nodeIndex = parseInt(nodeId.split('_')[1]);

    return {
      id: nodeId,
      role: 'looking',
      status: 'alive',
      currentTerm: 0,       // epoch
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
        epoch: 0,
        counter: 0,          // txn counter within epoch
        phase: 'election' as 'election' | 'synchronization' | 'broadcast',
        // Election
        proposedLeader: nodeId,
        electionVotes: {} as Record<string, { leader: string; epoch: number; counter: number }>,
        // Sync
        syncAcks: 0,
        followerInfoCount: 0,
        // Broadcast
        pendingProposals: {} as Record<string, {
          acks: number;
          epoch: number;
          counter: number;
          index: number;
          requestId: string;
          entryId: string;
          command: string;
        }>,
        pendingCommits: {} as Record<string, {
          epoch: number;
          counter: number;
          index: number;
          requestId: string;
          entryId: string;
        }>,
        commandQueue: [] as ClientRequest[],
        knownLeader: null as NodeId | null,
        heartbeatInterval: config.heartbeatInterval,
      },
    };
  }

  canAcceptClientRequest(node: NodeState): boolean {
    return node.role === 'leading' && (node.meta.phase as string) === 'broadcast' && node.status === 'alive';
  }

  getKnownLeader(node: NodeState): NodeId | null {
    if (node.role === 'leading') return node.id;
    return (node.meta.knownLeader as NodeId | null) ?? null;
  }

  onMessage(node: NodeState, msg: Message): Action[] {
    switch (msg.type) {
      case 'zab_election': return this.handleElectionVote(node, msg);
      case 'zab_election_ack': return this.handleElectionAck(node, msg);
      case 'zab_followerinfo': return this.handleFollowerInfo(node, msg);
      case 'zab_sync': return this.handleSync(node, msg);
      case 'zab_newleader': return this.handleNewLeader(node, msg);
      case 'zab_ack_newleader': return this.handleAckNewLeader(node, msg);
      case 'zab_proposal': return this.handleProposal(node, msg);
      case 'zab_ack': return this.handleAck(node, msg);
      case 'zab_commit': return this.handleCommit(node, msg);
      case 'mp_heartbeat': return this.handleHeartbeat(node, msg);
      default: return [];
    }
  }

  onTimeout(node: NodeState, type: TimeoutType): Action[] {
    if (type === 'election') {
      // Start or restart election
      return this.startElection(node);
    }
    if (type === 'heartbeat' && node.role === 'leading') {
      return this.sendHeartbeats(node);
    }
    return [];
  }

  onClientRequest(node: NodeState, request: ClientRequest): Action[] {
    if (node.role !== 'leading' || (node.meta.phase as string) !== 'broadcast') {
      return [{
        type: 'send_message',
        message: {
          type: 'client_response', from: node.id, to: node.id,
          term: node.currentTerm,
          payload: { success: false, redirect: true, leaderHint: this.getKnownLeader(node) },
        },
      }];
    }

    // Propose immediately
    node.meta.counter = (node.meta.counter as number) + 1;
    const epoch = node.meta.epoch as number;
    const counter = node.meta.counter as number;
    const zxidKey = `${epoch}:${counter}`;

    const proposals = node.meta.pendingProposals as Record<string, {
      acks: number;
      epoch: number;
      counter: number;
      index: number;
      requestId: string;
      entryId: string;
      command: string;
    }>;
    proposals[zxidKey] = {
      acks: 1,
      epoch,
      counter,
      index: node.logBaseIndex + node.log.length,
      requestId: request.requestId,
      entryId: request.entryId,
      command: request.command,
    }; // self-ack

    const entry: LogEntry = {
      entryId: request.entryId,
      requestId: request.requestId,
      zxid: zxidKey,
      term: epoch,
      index: proposals[zxidKey].index,
      command: request.command,
      committed: false,
    };
    node.log.push(entry);

    const peers = node.meta.peers as NodeId[];
    const actions: Action[] = [];
    for (const peer of peers) {
      actions.push({
        type: 'send_message',
        message: {
          type: 'zab_proposal', from: node.id, to: peer, term: epoch,
          payload: {
            epoch,
            counter,
            index: entry.index,
            value: request.command,
            requestId: request.requestId,
            entryId: request.entryId,
            zxidKey,
          },
        },
      });
    }
    return actions;
  }

  onRecovery(node: NodeState, config: ClusterConfig): Action[] {
    node.role = 'looking';
    node.meta.phase = 'election';
    node.meta.knownLeader = null;
    node.meta.proposedLeader = node.id;
    node.meta.syncAcks = 0;
    node.meta.followerInfoCount = 0;
    node.meta.pendingProposals = {};
    node.meta.pendingCommits = {};

    return [{
      type: 'set_timeout',
      timeout: { type: 'election', duration: this.randomTimeout(config), nodeId: node.id },
    }];
  }

  // ---- Phase 1: Election ----

  private startElection(node: NodeState): Action[] {
    node.role = 'looking';
    node.meta.phase = 'election';
    node.meta.proposedLeader = node.id;
    node.meta.knownLeader = null;
    node.meta.electionVotes = {};
    node.meta.pendingProposals = {};
    node.meta.pendingCommits = {};
    node.votesReceived.clear();
    node.votesReceived.add(node.id);

    // Vote for self
    const votes = node.meta.electionVotes as Record<string, { leader: string; epoch: number; counter: number }>;
    const epoch = node.meta.epoch as number;
    const counter = node.meta.counter as number;
    votes[node.id] = { leader: node.id, epoch, counter };

    const peers = node.meta.peers as NodeId[];
    const actions: Action[] = [];
    for (const peer of peers) {
      actions.push({
        type: 'send_message',
        message: {
          type: 'zab_election', from: node.id, to: peer,
          term: epoch,
          payload: { proposedLeader: node.id, epoch, counter },
        },
      });
    }

    actions.push({
      type: 'set_timeout',
      timeout: { type: 'election', duration: 300 + this.rng() * 300, nodeId: node.id },
    });

    return actions;
  }

  private handleElectionVote(node: NodeState, msg: Message): Action[] {
    if (node.role !== 'looking') return [];

    const { proposedLeader, epoch: vEpoch, counter: vCounter } = msg.payload as {
      proposedLeader: string; epoch: number; counter: number;
    };

    const myEpoch = node.meta.epoch as number;
    const myCounter = node.meta.counter as number;
    const myLeader = node.meta.proposedLeader as string;

    // Record sender's ACTUAL vote (what they proposed, not our preference)
    const votes = node.meta.electionVotes as Record<string, { leader: string; epoch: number; counter: number }>;
    votes[msg.from] = { leader: proposedLeader, epoch: vEpoch, counter: vCounter };
    node.votesReceived.add(msg.from);

    // Update our own vote if incoming proposal is better
    let updateVote = false;
    if (vEpoch > myEpoch) updateVote = true;
    else if (vEpoch === myEpoch && vCounter > myCounter) updateVote = true;
    else if (vEpoch === myEpoch && vCounter === myCounter && proposedLeader > myLeader) updateVote = true;

    const actions: Action[] = [];
    if (updateVote) {
      node.meta.proposedLeader = proposedLeader;
      node.meta.epoch = vEpoch;
      node.meta.counter = vCounter;

      // Update our own entry in votes table
      votes[node.id] = { leader: proposedLeader, epoch: vEpoch, counter: vCounter };

      // Re-broadcast our updated vote
      const peers = node.meta.peers as NodeId[];
      for (const peer of peers) {
        actions.push({
          type: 'send_message',
          message: {
            type: 'zab_election', from: node.id, to: peer, term: vEpoch,
            payload: { proposedLeader, epoch: vEpoch, counter: vCounter },
          },
        });
      }
    }

    // Check if quorum agrees on same leader
    const currentLeader = node.meta.proposedLeader as string;
    const peers = node.meta.peers as NodeId[];
    const majority = Math.floor((peers.length + 1) / 2) + 1;
    let agreementCount = 0;
    for (const v of Object.values(votes)) {
      if (v.leader === currentLeader) agreementCount++;
    }

    if (agreementCount >= majority) {
      actions.push(...this.finishElection(node, currentLeader));
    }

    return actions;
  }

  private handleElectionAck(_node: NodeState, _msg: Message): Action[] { // eslint-disable-line @typescript-eslint/no-unused-vars
    return [];
  }

  private finishElection(node: NodeState, leaderId: NodeId): Action[] {
    if (leaderId === node.id) {
      // We are the leader
      node.role = 'leading';
      node.meta.phase = 'synchronization';
      node.meta.epoch = (node.meta.epoch as number) + 1;
      node.currentTerm = node.meta.epoch as number;
      node.meta.counter = 0;
      node.meta.knownLeader = node.id;
      node.meta.syncAcks = 1; // self
      node.meta.followerInfoCount = 0;
      node.votesReceived.clear();
      node.votesReceived.add(node.id);

      return [
        { type: 'cancel_timeout', timeout: { type: 'election', duration: 0, nodeId: node.id } },
      ];
    } else {
      // We are a follower
      node.role = 'following';
      node.meta.phase = 'synchronization';
      node.meta.knownLeader = leaderId;

      // Send follower info to leader
      const epoch = node.meta.epoch as number;
      const counter = node.meta.counter as number;
      return [
        { type: 'cancel_timeout', timeout: { type: 'election', duration: 0, nodeId: node.id } },
        {
          type: 'send_message',
          message: {
            type: 'zab_followerinfo', from: node.id, to: leaderId, term: epoch,
            payload: {
              lastEpoch: epoch,
              lastCounter: counter,
              lastLogIndex: node.log.length > 0 ? node.log[node.log.length - 1].index : node.logBaseIndex - 1,
            },
          },
        },
      ];
    }
  }

  // ---- Phase 2: Synchronization ----

  private handleFollowerInfo(node: NodeState, msg: Message): Action[] {
    if (node.role !== 'leading') return [];

    const { lastLogIndex } = msg.payload as { lastEpoch: number; lastCounter: number; lastLogIndex: number };
    node.meta.followerInfoCount = (node.meta.followerInfoCount as number) + 1;

    const actions: Action[] = [];

    // Send missing log entries as sync
    if (lastLogIndex < node.commitIndex) {
      const missing = node.log.filter(e => e.committed && e.index > lastLogIndex);
      for (const entry of missing) {
        actions.push({
          type: 'send_message',
          message: {
            type: 'zab_sync', from: node.id, to: msg.from,
            term: node.currentTerm,
            payload: {
              entry: {
                entryId: entry.entryId,
                requestId: entry.requestId,
                zxid: entry.zxid ?? `${entry.term}:${entry.index}`,
                term: entry.term,
                index: entry.index,
                command: entry.command,
                committed: true,
              },
            },
          },
        });
      }
    }

    // Send NEWLEADER
    const epoch = node.meta.epoch as number;
    actions.push({
      type: 'send_message',
      message: {
        type: 'zab_newleader', from: node.id, to: msg.from, term: epoch,
        payload: { epoch },
      },
    });

    // Check if all followers responded
    const peers = node.meta.peers as NodeId[];
    const majority = Math.floor((peers.length + 1) / 2) + 1;
    if ((node.meta.syncAcks as number) >= majority) {
      actions.push(...this.enterBroadcast(node));
    }

    return actions;
  }

  private handleSync(node: NodeState, msg: Message): Action[] {
    if (node.role === 'leading') return [];  // only leader ignores sync

    const { entry } = msg.payload as { entry: LogEntry };
    this.upsertLogEntry(node, {
      entryId: entry.entryId,
      requestId: entry.requestId,
      zxid: entry.zxid,
      term: entry.term,
      index: entry.index,
      command: entry.command,
      committed: true,
    });
    this.trackZxid(node, entry.term, this.zxidCounter(entry.zxid, entry.index));
    return [];
  }

  private handleNewLeader(node: NodeState, msg: Message): Action[] {
    if (node.role !== 'following') return [];

    const { epoch } = msg.payload as { epoch: number };
    node.meta.epoch = epoch;
    node.currentTerm = epoch;

    return [{
      type: 'send_message',
      message: {
        type: 'zab_ack_newleader', from: node.id, to: msg.from, term: epoch,
        payload: { epoch },
      },
    }];
  }

  private handleAckNewLeader(node: NodeState, _msg: Message): Action[] {
    if (node.role !== 'leading') return [];

    node.meta.syncAcks = (node.meta.syncAcks as number) + 1;
    node.votesReceived.add(_msg.from);

    const peers = node.meta.peers as NodeId[];
    const majority = Math.floor((peers.length + 1) / 2) + 1;

    if ((node.meta.syncAcks as number) >= majority && (node.meta.phase as string) === 'synchronization') {
      return this.enterBroadcast(node);
    }
    return [];
  }

  private enterBroadcast(node: NodeState): Action[] {
    node.meta.phase = 'broadcast';
    node.meta.pendingProposals = {};

    return [{
      type: 'set_timeout',
      timeout: { type: 'heartbeat', duration: (node.meta.heartbeatInterval as number), nodeId: node.id },
    }];
  }

  // ---- Phase 3: Broadcast ----

  private handleProposal(node: NodeState, msg: Message): Action[] {
    if (node.role !== 'following') return [];

    const { epoch, counter, index, value, zxidKey } = msg.payload as {
      epoch: number; counter: number; index: number; value: string; zxidKey: string; requestId: string; entryId: string;
    };

    const entry: LogEntry = {
      entryId: (msg.payload.entryId as string),
      requestId: (msg.payload.requestId as string),
      zxid: zxidKey,
      term: epoch,
      index,
      command: value,
      committed: false,
    };
    const pendingCommits = node.meta.pendingCommits as Record<string, {
      epoch: number;
      counter: number;
      index: number;
      requestId: string;
      entryId: string;
    }>;
    const pendingCommit = pendingCommits[zxidKey];
    if (pendingCommit && pendingCommit.entryId === entry.entryId) {
      entry.committed = true;
      delete pendingCommits[zxidKey];
    }
    this.upsertLogEntry(node, entry);

    // Update epoch/counter tracking
    this.trackZxid(node, epoch, counter);

    return [{
      type: 'send_message',
      message: {
        type: 'zab_ack', from: node.id, to: msg.from, term: epoch,
        payload: { zxidKey, epoch, counter, index: entry.index, entryId: entry.entryId },
      },
    }];
  }

  private handleAck(node: NodeState, msg: Message): Action[] {
    if (node.role !== 'leading') return [];

    const { zxidKey } = msg.payload as { zxidKey: string };
    const proposals = node.meta.pendingProposals as Record<string, {
      acks: number;
      epoch: number;
      counter: number;
      index: number;
      requestId: string;
      entryId: string;
      command: string;
    }>;
    const prop = proposals[zxidKey];
    if (!prop) return [];

    prop.acks++;

    const peers = node.meta.peers as NodeId[];
    const majority = Math.floor((peers.length + 1) / 2) + 1;

    if (prop.acks >= majority) {
      delete proposals[zxidKey];

      // Mark committed in log for the specific proposal entry
      this.upsertLogEntry(node, {
        entryId: prop.entryId,
        requestId: prop.requestId,
        zxid: zxidKey,
        term: prop.epoch,
        index: prop.index,
        command: prop.command,
        committed: true,
      });

      const actions: Action[] = [{ type: 'commit_entry' }];

      // Broadcast commit
      for (const peer of peers) {
        actions.push({
          type: 'send_message',
          message: {
            type: 'zab_commit', from: node.id, to: peer, term: prop.epoch,
            payload: {
              zxidKey,
              epoch: prop.epoch,
              counter: prop.counter,
              index: prop.index,
              requestId: prop.requestId,
              entryId: prop.entryId,
            },
          },
        });
      }

      return actions;
    }
    return [];
  }

  private handleCommit(node: NodeState, _msg: Message): Action[] {
    const { zxidKey, epoch, counter, index, requestId, entryId } = _msg.payload as {
      zxidKey: string; epoch: number; counter: number; index: number; requestId: string; entryId: string;
    };

    const committedEntry = this.findLogEntry(node, zxidKey, entryId);
    if (committedEntry) {
      committedEntry.committed = true;
      committedEntry.index = index;
      committedEntry.zxid = zxidKey;
      node.commitIndex = Math.max(node.commitIndex, committedEntry.index);
    } else {
      const pendingCommits = node.meta.pendingCommits as Record<string, {
        epoch: number;
        counter: number;
        index: number;
        requestId: string;
        entryId: string;
      }>;
      pendingCommits[zxidKey] = { epoch, counter, index, requestId, entryId };
    }
    this.trackZxid(node, epoch, counter);

    // Remove from queue if present
    const queue = node.meta.commandQueue as ClientRequest[];
    // Not typically needed for followers but clean up
    const idx = queue.findIndex(v => v.requestId === requestId);
    if (idx !== -1) queue.splice(idx, 1);

    return [];
  }

  // ---- Heartbeats ----

  private sendHeartbeats(node: NodeState): Action[] {
    const peers = node.meta.peers as NodeId[];
    const actions: Action[] = [];
    for (const peer of peers) {
      actions.push({
        type: 'send_message',
        message: {
          type: 'mp_heartbeat', from: node.id, to: peer,
          term: node.currentTerm,
          payload: { epoch: node.meta.epoch, commitIndex: node.commitIndex },
        },
      });
    }
    actions.push({
      type: 'set_timeout',
      timeout: { type: 'heartbeat', duration: (node.meta.heartbeatInterval as number), nodeId: node.id },
    });
    return actions;
  }

  private handleHeartbeat(node: NodeState, msg: Message): Action[] {
    if (node.role === 'leading') return [];

    node.meta.knownLeader = msg.from;
    if (typeof msg.payload.epoch === 'number') {
      node.meta.epoch = Math.max(node.meta.epoch as number, msg.payload.epoch as number);
      node.currentTerm = node.meta.epoch as number;
    }
    if (node.role === 'looking') {
      node.role = 'following';
      node.meta.phase = 'broadcast';
    }

    return [{
      type: 'set_timeout',
      timeout: { type: 'election', duration: 300 + this.rng() * 300, nodeId: node.id },
    }];
  }

  private randomTimeout(config: ClusterConfig): number {
    return config.electionTimeoutMin + this.rng() * (config.electionTimeoutMax - config.electionTimeoutMin);
  }

  private upsertLogEntry(node: NodeState, entry: LogEntry): void {
    if (entry.index < node.logBaseIndex) return;

    const existingIndex = node.log.findIndex(candidate =>
      candidate.entryId === entry.entryId
      || (entry.zxid !== undefined && candidate.zxid === entry.zxid)
      || candidate.index === entry.index);

    if (existingIndex !== -1) {
      const current = node.log[existingIndex];
      node.log[existingIndex] = {
        ...current,
        ...entry,
        committed: current.committed || entry.committed,
      };
    } else {
      node.log.push(entry);
      node.log.sort((a, b) => a.index - b.index);
    }

    if (entry.committed) {
      node.commitIndex = Math.max(node.commitIndex, entry.index);
    }
  }

  private findLogEntry(node: NodeState, zxidKey: string, entryId: string): LogEntry | undefined {
    return node.log.find(entry => entry.entryId === entryId || entry.zxid === zxidKey);
  }

  private trackZxid(node: NodeState, epoch: number, counter: number): void {
    if (epoch > (node.meta.epoch as number) || (epoch === (node.meta.epoch as number) && counter > (node.meta.counter as number))) {
      node.meta.epoch = epoch;
      node.meta.counter = counter;
      node.currentTerm = epoch;
    }
  }

  private zxidCounter(zxid: string | undefined, fallbackIndex: number): number {
    if (!zxid) return fallbackIndex;
    const [, counter] = zxid.split(':');
    return Number.parseInt(counter ?? '', 10) || fallbackIndex;
  }
}
