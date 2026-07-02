import { ConsensusAlgorithm } from './interface';
import {
  NodeState, NodeId, Message, Action, ClusterConfig, TimeoutType, LogEntry, ClientRequest,
} from '../types';
import {
  PAXOS_PROPOSAL_BASE_TIMEOUT, PAXOS_PROPOSAL_PER_NODE_INCREMENT, PAXOS_PROPOSAL_JITTER,
  PAXOS_NACK_BACKOFF_BASE, PAXOS_NACK_BACKOFF_PER_NODE, PAXOS_NACK_BACKOFF_JITTER,
} from '../constants';

interface PaxosSlotState {
  minProposal: number;
  acceptedProposal: number;
  acceptedValue: ClientRequest | null;
}

/**
 * Classic (Basic) Paxos — no stable leader, no heartbeats.
 *
 * This implementation is slot-based: each committed log index is an
 * independent Paxos instance with its own prepare/accept state.
 */
export class PaxosAlgorithm implements ConsensusAlgorithm {
  readonly name = 'Paxos';
  readonly description = 'Classic quorum-based consensus — any node can propose, no stable leader';
  private rng: () => number = Math.random;

  setRandomSource(rng: () => number): void {
    this.rng = rng;
  }

  getInitialState(nodeId: NodeId, config: ClusterConfig): NodeState {
    const allNodes = Array.from({ length: config.nodeCount }, (_, i) => `node_${i}`);
    const nodeIndex = parseInt(nodeId.split('_')[1]);

    return {
      id: nodeId,
      role: 'acceptor',
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
        currentSlot: null as number | null,
        nextProposalSlot: 0,
        promisesReceived: 0,
        acceptsReceived: 0,
        highestPromisedProposal: 0,
        highestPromisedValue: null as ClientRequest | null,
        pendingValue: null as ClientRequest | null,
        isProposing: false,
        proposalPhase: null as 'prepare' | 'accept' | null,
        slotStates: {} as Record<number, PaxosSlotState>,
        // Legacy single-slot fields kept for UI/debug display.
        minProposal: 0,
        acceptedProposal: -1,
        acceptedValue: null as ClientRequest | null,
        commandQueue: [] as ClientRequest[],
      },
    };
  }

  canAcceptClientRequest(node: NodeState): boolean {
    return node.status === 'alive';
  }

  getKnownLeader(_node: NodeState): NodeId | null { // eslint-disable-line @typescript-eslint/no-unused-vars
    return null;
  }

  onMessage(node: NodeState, msg: Message): Action[] {
    switch (msg.type) {
      case 'prepare': return this.handlePrepare(node, msg);
      case 'promise': return this.handlePromise(node, msg);
      case 'accept': return this.handleAccept(node, msg);
      case 'accepted': return this.handleAccepted(node, msg);
      case 'nack': return this.handleNack(node, msg);
      case 'learn': return this.handleLearn(node, msg);
      default: return [];
    }
  }

  onTimeout(node: NodeState, type: TimeoutType): Action[] {
    if (type === 'election') {
      if (node.meta.isProposing || (node.meta.commandQueue as ClientRequest[]).length > 0) {
        return this.startProposal(node);
      }
    }
    return [];
  }

  onClientRequest(node: NodeState, request: ClientRequest): Action[] {
    (node.meta.commandQueue as ClientRequest[]).push(request);

    if (!node.meta.isProposing) {
      return this.startProposal(node);
    }

    return [];
  }

  onRecovery(node: NodeState, config: ClusterConfig): Action[] {
    node.role = 'acceptor';
    node.meta.isProposing = false;
    node.meta.proposalPhase = null;
    node.meta.promisesReceived = 0;
    node.meta.acceptsReceived = 0;
    node.meta.currentSlot = null;
    this.syncDisplayedAcceptorState(node, null);

    return [{
      type: 'set_timeout',
      timeout: { type: 'election', duration: this.randomTimeout(config), nodeId: node.id },
    }];
  }

  private startProposal(node: NodeState): Action[] {
    const queue = node.meta.commandQueue as ClientRequest[];
    if (queue.length === 0) {
      node.meta.isProposing = false;
      node.meta.currentSlot = null;
      node.role = 'acceptor';
      this.syncDisplayedAcceptorState(node, null);
      return [];
    }

    const peers = node.meta.peers as NodeId[];
    const nodeCount = peers.length + 1;
    const nodeIndex = node.meta.nodeIndex as number;
    const slot = (node.meta.currentSlot as number | null) ?? this.nextProposalSlot(node);

    node.meta.seqNum = (node.meta.seqNum as number) + 1;
    const proposalNumber = (node.meta.seqNum as number) * nodeCount + nodeIndex;

    node.meta.proposalNumber = proposalNumber;
    node.meta.currentSlot = slot;
    node.currentTerm = proposalNumber;
    node.role = 'proposer';
    node.meta.isProposing = true;
    node.meta.proposalPhase = 'prepare';
    node.meta.promisesReceived = 1;
    node.meta.acceptsReceived = 0;
    node.meta.pendingValue = queue[0];
    node.votesReceived.clear();
    node.votesReceived.add(node.id);

    const slotState = this.getSlotState(node, slot);
    const selfAcceptedProposal = slotState.acceptedProposal;
    const selfAcceptedValue = slotState.acceptedValue;
    const selfAlreadyCommitted = selfAcceptedValue !== null &&
      node.log.some(e => e.entryId === selfAcceptedValue.entryId && e.committed);
    if (selfAcceptedProposal > 0 && selfAcceptedValue !== null && !selfAlreadyCommitted) {
      node.meta.highestPromisedProposal = selfAcceptedProposal;
      node.meta.highestPromisedValue = selfAcceptedValue;
    } else {
      node.meta.highestPromisedProposal = 0;
      node.meta.highestPromisedValue = null;
    }

    if (proposalNumber > slotState.minProposal) {
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

    if (proposalNumber > slotState.minProposal) {
      slotState.minProposal = proposalNumber;
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
        term: slotState.minProposal,
        payload: {
          proposalNumber,
          slot,
          highestSeen: slotState.minProposal,
        },
      },
    }];
  }

  private handlePromise(node: NodeState, msg: Message): Action[] {
    if (!node.meta.isProposing || node.meta.proposalPhase !== 'prepare') return [];

    const { proposalNumber, slot, acceptedProposal, acceptedValue } = msg.payload as {
      proposalNumber: number;
      slot: number;
      acceptedProposal: number;
      acceptedValue: ClientRequest | null;
    };

    if (proposalNumber !== node.meta.proposalNumber || slot !== node.meta.currentSlot) return [];

    node.votesReceived.add(msg.from);
    node.meta.promisesReceived = (node.meta.promisesReceived as number) + 1;

    if (acceptedProposal > (node.meta.highestPromisedProposal as number) && acceptedValue !== null) {
      const alreadyCommitted = node.log.some(e => e.entryId === acceptedValue.entryId && e.committed);
      if (!alreadyCommitted) {
        node.meta.highestPromisedProposal = acceptedProposal;
        node.meta.highestPromisedValue = acceptedValue;
      }
    }

    const peers = node.meta.peers as NodeId[];
    const majority = Math.floor((peers.length + 1) / 2) + 1;

    if ((node.meta.promisesReceived as number) >= majority) {
      node.meta.proposalPhase = 'accept';
      const valueToPropose = (node.meta.highestPromisedValue as ClientRequest | null)
        ?? (node.meta.pendingValue as ClientRequest | null);
      if (!valueToPropose) return [];
      return this.sendAccept(node, slot, valueToPropose);
    }

    return [];
  }

  private sendAccept(node: NodeState, slot: number, value: ClientRequest): Action[] {
    const peers = node.meta.peers as NodeId[];
    const proposalNumber = node.meta.proposalNumber as number;
    const slotState = this.getSlotState(node, slot);
    const actions: Action[] = [];

    for (const peer of peers) {
      actions.push({
        type: 'send_message',
        message: {
          type: 'accept',
          from: node.id,
          to: peer,
          term: proposalNumber,
          payload: { proposalNumber, slot, value },
        },
      });
    }

    slotState.acceptedProposal = proposalNumber;
    slotState.acceptedValue = value;
    this.syncDisplayedAcceptorState(node, slot);
    node.meta.acceptsReceived = 1;

    return actions;
  }

  private handleAccept(node: NodeState, msg: Message): Action[] {
    const { proposalNumber, slot, value } = msg.payload as {
      proposalNumber: number;
      slot: number;
      value: ClientRequest;
    };
    const slotState = this.getSlotState(node, slot);

    if (proposalNumber >= slotState.minProposal) {
      slotState.minProposal = proposalNumber;
      slotState.acceptedProposal = proposalNumber;
      slotState.acceptedValue = value;
      this.syncDisplayedAcceptorState(node, slot);

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
        term: slotState.minProposal,
        payload: { proposalNumber, slot, highestSeen: slotState.minProposal },
      },
    }];
  }

  private handleAccepted(node: NodeState, msg: Message): Action[] {
    if (!node.meta.isProposing) return [];

    const { proposalNumber, slot, value } = msg.payload as {
      proposalNumber: number;
      slot: number;
      value: ClientRequest;
    };

    if (proposalNumber !== node.meta.proposalNumber || slot !== node.meta.currentSlot) return [];

    node.meta.acceptsReceived = (node.meta.acceptsReceived as number) + 1;

    const peers = node.meta.peers as NodeId[];
    const majority = Math.floor((peers.length + 1) / 2) + 1;

    if ((node.meta.acceptsReceived as number) < majority) return [];

    node.meta.acceptsReceived = 0;
    const queue = node.meta.commandQueue as ClientRequest[];
    const idx = queue.findIndex(v => v.requestId === value.requestId);
    if (idx !== -1) queue.splice(idx, 1);

    node.meta.isProposing = false;
    node.meta.pendingValue = null;
    node.meta.proposalPhase = null;
    node.meta.currentSlot = null;
    node.meta.nextProposalSlot = Math.max(node.meta.nextProposalSlot as number, slot + 1);
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
      actions.push(...this.startProposal(node));
    } else {
      node.role = 'acceptor';
      actions.push({
        type: 'cancel_timeout',
        timeout: { type: 'election', duration: 0, nodeId: node.id },
      });
    }

    return actions;
  }

  private handleNack(node: NodeState, msg: Message): Action[] {
    if (!node.meta.isProposing) return [];

    const { proposalNumber, slot, highestSeen } = msg.payload as {
      proposalNumber: number;
      slot: number;
      highestSeen: number;
    };

    if (proposalNumber !== node.meta.proposalNumber || slot !== node.meta.currentSlot) return [];

    const nodeCount = (node.meta.peers as NodeId[]).length + 1;
    const nodeIndex = node.meta.nodeIndex as number;
    const minSeq = Math.ceil((highestSeen - nodeIndex) / nodeCount) + 1;
    if (minSeq > (node.meta.seqNum as number)) {
      node.meta.seqNum = minSeq;
    }

    node.meta.isProposing = false;
    node.meta.proposalPhase = null;
    node.role = 'acceptor';

    const backoff = PAXOS_NACK_BACKOFF_BASE + nodeIndex * PAXOS_NACK_BACKOFF_PER_NODE + this.rng() * PAXOS_NACK_BACKOFF_JITTER;
    return [{
      type: 'set_timeout',
      timeout: {
        type: 'election',
        duration: backoff,
        nodeId: node.id,
      },
    }];
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

    const queue = node.meta.commandQueue as ClientRequest[];
    const idx = queue.findIndex(v => v.requestId === value.requestId);
    if (idx !== -1) queue.splice(idx, 1);
    this.syncDisplayedAcceptorState(node, null);

    if ((node.meta.currentSlot as number | null) === learnedSlot) {
      node.meta.isProposing = false;
      node.meta.pendingValue = null;
      node.meta.proposalPhase = null;
      node.meta.currentSlot = null;
      if (queue.length > 0) {
        return this.startProposal(node);
      }
      node.role = 'acceptor';
      return [{
        type: 'cancel_timeout',
        timeout: { type: 'election', duration: 0, nodeId: node.id },
      }];
    }

    return [];
  }

  private getSlotState(node: NodeState, slot: number): PaxosSlotState {
    const slotStates = node.meta.slotStates as Record<number, PaxosSlotState>;
    if (!slotStates[slot]) {
      slotStates[slot] = {
        minProposal: 0,
        acceptedProposal: -1,
        acceptedValue: null,
      };
    }
    return slotStates[slot];
  }

  private syncDisplayedAcceptorState(node: NodeState, slot: number | null): void {
    if (slot === null) {
      node.meta.minProposal = 0;
      node.meta.acceptedProposal = -1;
      node.meta.acceptedValue = null;
      return;
    }

    const slotState = this.getSlotState(node, slot);
    node.meta.minProposal = slotState.minProposal;
    node.meta.acceptedProposal = slotState.acceptedProposal;
    node.meta.acceptedValue = slotState.acceptedValue;
  }

  private nextProposalSlot(node: NodeState): number {
    const lastLogIndex = node.log.length > 0 ? node.log[node.log.length - 1].index : node.logBaseIndex - 1;
    return Math.max(
      node.meta.nextProposalSlot as number,
      node.commitIndex + 1,
      node.logBaseIndex,
      lastLogIndex + 1,
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
