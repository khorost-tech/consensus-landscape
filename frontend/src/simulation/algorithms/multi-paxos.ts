import { ConsensusAlgorithm } from './interface';
import {
  NodeState, NodeId, Message, Action, ClusterConfig, TimeoutType, LogEntry, ClientRequest,
} from '../types';
import {
  PAXOS_PROPOSAL_BASE_TIMEOUT, PAXOS_PROPOSAL_PER_NODE_INCREMENT, PAXOS_PROPOSAL_JITTER,
  PAXOS_NACK_BACKOFF_BASE, PAXOS_NACK_BACKOFF_PER_NODE, PAXOS_NACK_BACKOFF_JITTER,
} from '../constants';

interface MultiPaxosSlotState {
  minProposal: number;
  acceptedProposal: number;
  acceptedValue: ClientRequest | null;
}

/**
 * Multi-Paxos — optimized Paxos with a stable leader.
 *
 * This variant is slot-based: each log index is its own Paxos instance.
 * The elected leader reuses one ballot across multiple slots and skips
 * Prepare after the first successful commit under that ballot.
 */
export class MultiPaxosAlgorithm implements ConsensusAlgorithm {
  readonly name = 'Multi-Paxos';
  readonly description = 'Paxos with stable leader — skip Prepare phase after election';
  private rng: () => number = Math.random;

  setRandomSource(rng: () => number): void {
    this.rng = rng;
  }

  getInitialState(nodeId: NodeId, config: ClusterConfig): NodeState {
    const allNodes = Array.from({ length: config.nodeCount }, (_, i) => `node_${i}`);
    const nodeIndex = parseInt(nodeId.split('_')[1]);

    return {
      id: nodeId,
      role: 'follower',
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
        seqNum: 0,
        proposalNumber: 0,
        leaderBallot: 0,
        promisesReceived: 0,
        acceptsReceived: 0,
        highestPromisedProposal: 0,
        highestPromisedValue: null as ClientRequest | null,
        isElecting: false,
        knownLeader: null as NodeId | null,
        commandQueue: [] as ClientRequest[],
        proposalPhase: null as 'prepare' | 'accept' | null,
        pendingValue: null as ClientRequest | null,
        currentSlot: null as number | null,
        nextProposalSlot: 0,
        leaderEstablished: false,
        slotStates: {} as Record<number, MultiPaxosSlotState>,
        // Legacy single-slot fields kept for UI/debug display.
        minProposal: 0,
        acceptedProposal: -1,
        acceptedValue: null as ClientRequest | null,
        heartbeatInterval: config.heartbeatInterval,
      },
    };
  }

  canAcceptClientRequest(node: NodeState): boolean {
    return node.role === 'leader' && node.status === 'alive';
  }

  getKnownLeader(node: NodeState): NodeId | null {
    if (node.role === 'leader') return node.id;
    return (node.meta.knownLeader as NodeId | null) ?? null;
  }

  onMessage(node: NodeState, msg: Message): Action[] {
    switch (msg.type) {
      case 'prepare': return this.handlePrepare(node, msg);
      case 'promise': return this.handlePromise(node, msg);
      case 'accept': return this.handleAccept(node, msg);
      case 'accepted': return this.handleAccepted(node, msg);
      case 'nack': return this.handleNack(node, msg);
      case 'learn': return this.handleLearn(node, msg);
      case 'mp_heartbeat': return this.handleHeartbeat(node, msg);
      case 'mp_heartbeat_response': return [];
      default: return [];
    }
  }

  onTimeout(node: NodeState, type: TimeoutType): Action[] {
    if (type === 'election') {
      return this.startElection(node);
    }
    if (type === 'heartbeat' && node.role === 'leader') {
      return this.sendHeartbeats(node);
    }
    return [];
  }

  onClientRequest(node: NodeState, request: ClientRequest): Action[] {
    if (node.role !== 'leader') {
      return [{
        type: 'send_message',
        message: {
          type: 'client_response',
          from: node.id,
          to: node.id,
          term: node.currentTerm,
          payload: { success: false, redirect: true, leaderHint: this.getKnownLeader(node) },
        },
      }];
    }

    (node.meta.commandQueue as ClientRequest[]).push(request);

    if (!node.meta.pendingValue) {
      return this.proposeNext(node);
    }
    return [];
  }

  onRecovery(node: NodeState, config: ClusterConfig): Action[] {
    node.role = 'follower';
    node.meta.isElecting = false;
    node.meta.proposalPhase = null;
    node.meta.pendingValue = null;
    node.meta.promisesReceived = 0;
    node.meta.acceptsReceived = 0;
    node.meta.currentSlot = null;
    node.meta.knownLeader = null;
    node.meta.leaderEstablished = false;
    this.syncDisplayedAcceptorState(node, null);

    return [{
      type: 'set_timeout',
      timeout: { type: 'election', duration: this.randomTimeout(config), nodeId: node.id },
    }];
  }

  private startElection(node: NodeState): Action[] {
    const peers = node.meta.peers as NodeId[];
    const nodeCount = peers.length + 1;
    const nodeIndex = node.meta.nodeIndex as number;
    const slot = this.nextProposalSlot(node);

    node.meta.seqNum = (node.meta.seqNum as number) + 1;
    const proposalNumber = (node.meta.seqNum as number) * nodeCount + nodeIndex;

    node.meta.proposalNumber = proposalNumber;
    node.meta.currentSlot = slot;
    node.currentTerm = proposalNumber;
    node.role = 'candidate';
    node.meta.isElecting = true;
    node.meta.proposalPhase = 'prepare';
    node.meta.promisesReceived = 1;
    node.meta.acceptsReceived = 0;
    node.meta.knownLeader = null;
    node.votesReceived.clear();
    node.votesReceived.add(node.id);

    const slotState = this.getSlotState(node, slot);
    const selfAcceptedProposal = slotState.acceptedProposal;
    const selfAcceptedValue = slotState.acceptedValue;
    const selfAlreadyCommitted = selfAcceptedValue !== null
      && node.log.some(entry => entry.entryId === selfAcceptedValue.entryId && entry.committed);

    if (selfAcceptedProposal > 0 && selfAcceptedValue !== null && !selfAlreadyCommitted) {
      node.meta.highestPromisedProposal = selfAcceptedProposal;
      node.meta.highestPromisedValue = selfAcceptedValue;
    } else {
      node.meta.highestPromisedProposal = 0;
      node.meta.highestPromisedValue = null;
    }

    const promised = this.getPromisedProposal(node, slot);
    if (proposalNumber > promised) {
      node.meta.minProposal = proposalNumber;
      slotState.minProposal = proposalNumber;
    }
    this.syncDisplayedAcceptorState(node, slot);

    const actions: Action[] = [];
    for (const peer of peers) {
      actions.push({
        type: 'send_message',
        message: {
          type: 'prepare',
          from: node.id,
          to: peer,
          term: proposalNumber,
          payload: { proposalNumber, slot },
        },
      });
    }

    const baseDuration = PAXOS_PROPOSAL_BASE_TIMEOUT + nodeIndex * PAXOS_PROPOSAL_PER_NODE_INCREMENT;
    actions.push({
      type: 'set_timeout',
      timeout: {
        type: 'election',
        duration: baseDuration + this.rng() * PAXOS_PROPOSAL_JITTER,
        nodeId: node.id,
      },
    });

    return actions;
  }

  private handlePrepare(node: NodeState, msg: Message): Action[] {
    const { proposalNumber, slot } = msg.payload as { proposalNumber: number; slot: number };
    const slotState = this.getSlotState(node, slot);
    const promised = this.getPromisedProposal(node, slot);

    if (proposalNumber > promised) {
      node.meta.minProposal = proposalNumber;
      slotState.minProposal = proposalNumber;

      if (node.role === 'leader' && proposalNumber > (node.meta.leaderBallot as number)) {
        node.role = 'follower';
        node.meta.knownLeader = null;
        node.meta.pendingValue = null;
        node.meta.proposalPhase = null;
        node.meta.currentSlot = null;
        node.meta.leaderEstablished = false;
      }

      this.syncDisplayedAcceptorState(node, slot);

      return [{
        type: 'send_message',
        message: {
          type: 'promise',
          from: node.id,
          to: msg.from,
          term: proposalNumber,
          payload: {
            proposalNumber,
            slot,
            acceptedProposal: slotState.acceptedProposal,
            acceptedValue: slotState.acceptedValue,
          },
        },
      }];
    }

    return [{
      type: 'send_message',
      message: {
        type: 'nack',
        from: node.id,
        to: msg.from,
        term: promised,
        payload: { proposalNumber, slot, highestSeen: promised },
      },
    }];
  }

  private handlePromise(node: NodeState, msg: Message): Action[] {
    if (node.meta.proposalPhase !== 'prepare') return [];

    const { proposalNumber, slot, acceptedProposal, acceptedValue } = msg.payload as {
      proposalNumber: number;
      slot: number;
      acceptedProposal: number;
      acceptedValue: ClientRequest | null;
    };

    const expectedBallot = node.meta.isElecting
      ? node.meta.proposalNumber as number
      : node.meta.leaderBallot as number;
    if (proposalNumber !== expectedBallot || slot !== node.meta.currentSlot) return [];

    node.votesReceived.add(msg.from);
    node.meta.promisesReceived = (node.meta.promisesReceived as number) + 1;

    if (acceptedProposal > (node.meta.highestPromisedProposal as number) && acceptedValue !== null) {
      const alreadyCommitted = node.log.some(entry => entry.entryId === acceptedValue.entryId && entry.committed);
      if (!alreadyCommitted) {
        node.meta.highestPromisedProposal = acceptedProposal;
        node.meta.highestPromisedValue = acceptedValue;
      }
    }

    const peers = node.meta.peers as NodeId[];
    const majority = Math.floor((peers.length + 1) / 2) + 1;

    if ((node.meta.promisesReceived as number) < majority) return [];

    if (node.meta.isElecting) {
      return this.becomeLeader(node);
    }

    return this.sendAcceptForPending(node);
  }

  private becomeLeader(node: NodeState): Action[] {
    node.role = 'leader';
    node.meta.isElecting = false;
    node.meta.proposalPhase = null;
    node.meta.leaderBallot = node.meta.proposalNumber;
    node.meta.leaderEstablished = false;
    node.meta.knownLeader = node.id;
    node.meta.currentSlot = null;
    this.syncDisplayedAcceptorState(node, null);

    const actions: Action[] = [
      { type: 'cancel_timeout', timeout: { type: 'election', duration: 0, nodeId: node.id } },
    ];

    actions.push(...this.sendHeartbeats(node));

    const adoptedValue = node.meta.highestPromisedValue as ClientRequest | null;
    if (adoptedValue !== null) {
      const alreadyCommitted = node.log.some(entry => entry.entryId === adoptedValue.entryId && entry.committed);
      if (!alreadyCommitted) {
        const queue = node.meta.commandQueue as ClientRequest[];
        if (!queue.some(request => request.requestId === adoptedValue.requestId)) {
          queue.unshift(adoptedValue);
        }
      }
    }
    node.meta.highestPromisedProposal = 0;
    node.meta.highestPromisedValue = null;

    if ((node.meta.commandQueue as ClientRequest[]).length > 0) {
      actions.push(...this.proposeNext(node));
    }

    return actions;
  }

  private proposeNext(node: NodeState): Action[] {
    const queue = node.meta.commandQueue as ClientRequest[];
    if (queue.length === 0 || node.role !== 'leader') {
      node.meta.pendingValue = null;
      node.meta.currentSlot = null;
      return [];
    }

    const slot = Math.max(
      (node.meta.currentSlot as number | null) ?? -1,
      this.nextProposalSlot(node),
    );
    const value = queue[0];
    node.meta.pendingValue = value;
    node.meta.currentSlot = slot;

    const ballot = node.meta.leaderBallot as number;
    const peers = node.meta.peers as NodeId[];
    const actions: Action[] = [];

    if (!node.meta.leaderEstablished) {
      node.meta.proposalPhase = 'prepare';
      node.meta.promisesReceived = 1;
      node.meta.acceptsReceived = 0;

      const slotState = this.getSlotState(node, slot);
      const selfAcceptedProposal = slotState.acceptedProposal;
      const selfAcceptedValue = slotState.acceptedValue;
      const selfAlreadyCommitted = selfAcceptedValue !== null
        && node.log.some(entry => entry.entryId === selfAcceptedValue.entryId && entry.committed);

      if (selfAcceptedProposal > 0 && selfAcceptedValue !== null && !selfAlreadyCommitted) {
        node.meta.highestPromisedProposal = selfAcceptedProposal;
        node.meta.highestPromisedValue = selfAcceptedValue;
      } else {
        node.meta.highestPromisedProposal = 0;
        node.meta.highestPromisedValue = null;
      }

      const promised = this.getPromisedProposal(node, slot);
      if (ballot > promised) {
        node.meta.minProposal = ballot;
        slotState.minProposal = ballot;
      }
      this.syncDisplayedAcceptorState(node, slot);

      for (const peer of peers) {
        actions.push({
          type: 'send_message',
          message: {
            type: 'prepare',
            from: node.id,
            to: peer,
            term: ballot,
            payload: { proposalNumber: ballot, slot },
          },
        });
      }
      return actions;
    }

    return this.sendAccept(node, slot, value);
  }

  private sendAcceptForPending(node: NodeState): Action[] {
    const value = node.meta.pendingValue as ClientRequest | null;
    const slot = node.meta.currentSlot as number | null;
    if (!value || node.role !== 'leader' || slot === null) return [];

    const adoptedValue = node.meta.highestPromisedValue as ClientRequest | null;
    const valueToAccept = adoptedValue ?? value;
    node.meta.highestPromisedProposal = 0;
    node.meta.highestPromisedValue = null;

    return this.sendAccept(node, slot, valueToAccept);
  }

  private sendAccept(node: NodeState, slot: number, value: ClientRequest): Action[] {
    node.meta.proposalPhase = 'accept';
    node.meta.acceptsReceived = 1;

    const ballot = node.meta.leaderBallot as number;
    const peers = node.meta.peers as NodeId[];
    const slotState = this.getSlotState(node, slot);
    slotState.minProposal = Math.max(slotState.minProposal, ballot);
    slotState.acceptedProposal = ballot;
    slotState.acceptedValue = value;
    node.meta.minProposal = Math.max(node.meta.minProposal as number, ballot);
    this.syncDisplayedAcceptorState(node, slot);

    const actions: Action[] = [];
    for (const peer of peers) {
      actions.push({
        type: 'send_message',
        message: {
          type: 'accept',
          from: node.id,
          to: peer,
          term: ballot,
          payload: { proposalNumber: ballot, slot, value },
        },
      });
    }
    return actions;
  }

  private handleAccept(node: NodeState, msg: Message): Action[] {
    const { proposalNumber, slot, value } = msg.payload as {
      proposalNumber: number;
      slot: number;
      value: ClientRequest;
    };
    const slotState = this.getSlotState(node, slot);
    const promised = this.getPromisedProposal(node, slot);

    if (proposalNumber >= promised) {
      node.meta.minProposal = proposalNumber;
      slotState.minProposal = proposalNumber;
      slotState.acceptedProposal = proposalNumber;
      slotState.acceptedValue = value;
      this.syncDisplayedAcceptorState(node, slot);

      if (node.role !== 'leader') {
        node.role = 'follower';
        node.meta.knownLeader = msg.from;
      }

      return [{
        type: 'send_message',
        message: {
          type: 'accepted',
          from: node.id,
          to: msg.from,
          term: proposalNumber,
          payload: { proposalNumber, slot, value },
        },
      }];
    }

    return [{
      type: 'send_message',
      message: {
        type: 'nack',
        from: node.id,
        to: msg.from,
        term: promised,
        payload: { proposalNumber, slot, highestSeen: promised },
      },
    }];
  }

  private handleAccepted(node: NodeState, msg: Message): Action[] {
    if (node.role !== 'leader') return [];

    const { proposalNumber, slot, value } = msg.payload as {
      proposalNumber: number;
      slot: number;
      value: ClientRequest;
    };
    if (proposalNumber !== node.meta.leaderBallot || slot !== node.meta.currentSlot) return [];

    node.meta.acceptsReceived = (node.meta.acceptsReceived as number) + 1;

    const peers = node.meta.peers as NodeId[];
    const majority = Math.floor((peers.length + 1) / 2) + 1;

    if ((node.meta.acceptsReceived as number) < majority) return [];

    node.meta.acceptsReceived = 0;

    const queue = node.meta.commandQueue as ClientRequest[];
    const idx = queue.findIndex(request => request.requestId === value.requestId);
    if (idx !== -1) queue.splice(idx, 1);

    node.meta.pendingValue = null;
    node.meta.proposalPhase = null;
    node.meta.currentSlot = null;
    node.meta.nextProposalSlot = Math.max(node.meta.nextProposalSlot as number, slot + 1);
    node.meta.leaderEstablished = true;
    this.syncDisplayedAcceptorState(node, null);

    const actions: Action[] = [];
    const committed = this.upsertCommittedEntry(node, slot, value, proposalNumber);
    if (committed) {
      actions.push({ type: 'commit_entry' });

      for (const peer of peers) {
        actions.push({
          type: 'send_message',
          message: {
            type: 'learn',
            from: node.id,
            to: peer,
            term: proposalNumber,
            payload: { slot, value, proposalNumber, commitIndex: slot },
          },
        });
      }
    }

    if (queue.length > 0) {
      actions.push(...this.proposeNext(node));
    }

    return actions;
  }

  private handleNack(node: NodeState, msg: Message): Action[] {
    const { proposalNumber, highestSeen } = msg.payload as {
      proposalNumber: number;
      slot?: number;
      highestSeen: number;
    };

    if (node.role === 'leader' && proposalNumber === node.meta.leaderBallot) {
      node.role = 'follower';
      node.meta.knownLeader = null;
      node.meta.pendingValue = null;
      node.meta.proposalPhase = null;
      node.meta.currentSlot = null;
      node.meta.leaderEstablished = false;
      this.syncDisplayedAcceptorState(node, null);

      const nodeCount = (node.meta.peers as NodeId[]).length + 1;
      const nodeIndex = node.meta.nodeIndex as number;
      const minSeq = Math.ceil((highestSeen - nodeIndex) / nodeCount) + 1;
      if (minSeq > (node.meta.seqNum as number)) node.meta.seqNum = minSeq;

      const backoff = PAXOS_NACK_BACKOFF_BASE
        + nodeIndex * PAXOS_NACK_BACKOFF_PER_NODE
        + this.rng() * PAXOS_NACK_BACKOFF_JITTER;
      return [
        { type: 'cancel_timeout', timeout: { type: 'heartbeat', duration: 0, nodeId: node.id } },
        { type: 'set_timeout', timeout: { type: 'election', duration: backoff, nodeId: node.id } },
      ];
    }

    if (node.meta.isElecting && proposalNumber === node.meta.proposalNumber) {
      node.meta.isElecting = false;
      node.meta.proposalPhase = null;
      node.meta.currentSlot = null;
      node.role = 'follower';
      this.syncDisplayedAcceptorState(node, null);

      const nodeCount = (node.meta.peers as NodeId[]).length + 1;
      const nodeIndex = node.meta.nodeIndex as number;
      const minSeq = Math.ceil((highestSeen - nodeIndex) / nodeCount) + 1;
      if (minSeq > (node.meta.seqNum as number)) node.meta.seqNum = minSeq;

      const backoff = PAXOS_NACK_BACKOFF_BASE
        + nodeIndex * PAXOS_NACK_BACKOFF_PER_NODE
        + this.rng() * PAXOS_NACK_BACKOFF_JITTER;
      return [{ type: 'set_timeout', timeout: { type: 'election', duration: backoff, nodeId: node.id } }];
    }

    return [];
  }

  private handleLearn(node: NodeState, msg: Message): Action[] {
    const { slot, value, proposalNumber } = msg.payload as {
      slot?: number;
      value: ClientRequest;
      proposalNumber: number;
      commitIndex?: number;
    };
    const learnedSlot = slot ?? (msg.payload.commitIndex as number);

    const committed = this.upsertCommittedEntry(node, learnedSlot, value, proposalNumber);
    void committed;
    node.meta.nextProposalSlot = Math.max(node.meta.nextProposalSlot as number, learnedSlot + 1);
    this.syncDisplayedAcceptorState(node, null);

    const queue = node.meta.commandQueue as ClientRequest[];
    const idx = queue.findIndex(request => request.requestId === value.requestId);
    if (idx !== -1) queue.splice(idx, 1);

    if ((node.meta.currentSlot as number | null) === learnedSlot) {
      node.meta.pendingValue = null;
      node.meta.proposalPhase = null;
      node.meta.currentSlot = null;

      if (node.role === 'leader') {
        node.meta.leaderEstablished = true;
        if (queue.length > 0) {
          return this.proposeNext(node);
        }
      }
    }

    return [];
  }

  private sendHeartbeats(node: NodeState): Action[] {
    const peers = node.meta.peers as NodeId[];
    const actions: Action[] = [];
    for (const peer of peers) {
      actions.push({
        type: 'send_message',
        message: {
          type: 'mp_heartbeat',
          from: node.id,
          to: peer,
          term: node.meta.leaderBallot as number,
          payload: { leaderBallot: node.meta.leaderBallot, commitIndex: node.commitIndex },
        },
      });
    }
    actions.push({
      type: 'set_timeout',
      timeout: { type: 'heartbeat', duration: node.meta.heartbeatInterval as number, nodeId: node.id },
    });
    return actions;
  }

  private handleHeartbeat(node: NodeState, msg: Message): Action[] {
    const { leaderBallot } = msg.payload as { leaderBallot: number };

    if (leaderBallot >= (node.meta.minProposal as number)) {
      node.meta.minProposal = leaderBallot;
      node.meta.knownLeader = msg.from;
      node.meta.isElecting = false;
      if (node.role !== 'leader' || msg.from !== node.id) {
        node.role = 'follower';
        node.meta.proposalPhase = null;
        node.meta.pendingValue = null;
        node.meta.currentSlot = null;
        node.meta.leaderEstablished = false;
      }

      return [
        { type: 'set_timeout', timeout: { type: 'election', duration: 300 + this.rng() * 300, nodeId: node.id } },
        {
          type: 'send_message',
          message: {
            type: 'mp_heartbeat_response',
            from: node.id,
            to: msg.from,
            term: leaderBallot,
            payload: {},
          },
        },
      ];
    }
    return [];
  }

  private getSlotState(node: NodeState, slot: number): MultiPaxosSlotState {
    const slotStates = node.meta.slotStates as Record<number, MultiPaxosSlotState>;
    if (!slotStates[slot]) {
      slotStates[slot] = {
        minProposal: node.meta.minProposal as number,
        acceptedProposal: -1,
        acceptedValue: null,
      };
    }
    return slotStates[slot];
  }

  private getPromisedProposal(node: NodeState, slot: number): number {
    const slotState = this.getSlotState(node, slot);
    return Math.max(node.meta.minProposal as number, slotState.minProposal);
  }

  private syncDisplayedAcceptorState(node: NodeState, slot: number | null): void {
    if (slot === null) {
      node.meta.acceptedProposal = -1;
      node.meta.acceptedValue = null;
      return;
    }

    const slotState = this.getSlotState(node, slot);
    node.meta.minProposal = Math.max(node.meta.minProposal as number, slotState.minProposal);
    node.meta.acceptedProposal = slotState.acceptedProposal;
    node.meta.acceptedValue = slotState.acceptedValue;
  }

  private nextProposalSlot(node: NodeState): number {
    return Math.max(
      node.meta.nextProposalSlot as number,
      node.commitIndex + 1,
      node.logBaseIndex,
    );
  }

  private upsertCommittedEntry(node: NodeState, slot: number, value: ClientRequest, proposalNumber: number): boolean {
    if (slot < node.logBaseIndex) return false;

    const entry: LogEntry = {
      entryId: value.entryId,
      requestId: value.requestId,
      term: proposalNumber,
      index: slot,
      command: value.command,
      committed: true,
    };

    const existingIndex = node.log.findIndex(logEntry => logEntry.index === slot);
    if (existingIndex !== -1) {
      const existing = node.log[existingIndex];
      if (existing.entryId === value.entryId && existing.committed) {
        node.commitIndex = Math.max(node.commitIndex, slot);
        return false;
      }
      node.log[existingIndex] = entry;
    } else {
      node.log.push(entry);
      node.log.sort((a, b) => a.index - b.index);
    }

    node.commitIndex = Math.max(node.commitIndex, slot);
    return true;
  }

  private randomTimeout(config: ClusterConfig): number {
    return config.electionTimeoutMin + this.rng() * (config.electionTimeoutMax - config.electionTimeoutMin);
  }
}
