import { describe, it, expect } from 'vitest';
import { PaxosAlgorithm } from './paxos';
import { ClusterConfig, NodeState, Message, ClientRequest } from '../types';

const paxos = new PaxosAlgorithm();

function makeConfig(nodeCount = 3): ClusterConfig {
  return {
    nodeCount,
    observerIds: [],
    networkConfig: { minDelay: 5, maxDelay: 10, packetLossRate: 0, partitions: [] },
    electionTimeoutMin: 50,
    electionTimeoutMax: 100,
    heartbeatInterval: 20,
    clientCount: 1,
  };
}

function makeNode(id: string, config = makeConfig()): NodeState {
  return paxos.getInitialState(id, config);
}

function msg(overrides: Partial<Message> & { type: Message['type']; from: string; to: string }): Message {
  return { id: 'test', term: 0, payload: {}, ...overrides };
}

function req(command: string, id = command): ClientRequest {
  return {
    requestId: `req_${id}`,
    entryId: `entry_${id}`,
    command,
  };
}

describe('PaxosAlgorithm', () => {
  describe('getInitialState', () => {
    it('starts as acceptor', () => {
      const node = makeNode('node_0');
      expect(node.role).toBe('acceptor');
      expect(node.meta.isProposing).toBe(false);
      expect(node.meta.proposalPhase).toBeNull();
    });

    it('sets nodeIndex from id', () => {
      const node = makeNode('node_2');
      expect(node.meta.nodeIndex).toBe(2);
    });
  });

  describe('client request → proposal', () => {
    it('queues command and starts proposal', () => {
      const node = makeNode('node_0');
      const actions = paxos.onClientRequest(node, req('cmd_1'));
      expect(node.role).toBe('proposer');
      expect(node.meta.isProposing).toBe(true);
      expect(node.meta.proposalPhase).toBe('prepare');
      // Should send prepare to 2 peers
      const prepares = actions.filter(a => a.message?.type === 'prepare');
      expect(prepares.length).toBe(2);
    });

    it('queues second command without starting new proposal', () => {
      const node = makeNode('node_0');
      paxos.onClientRequest(node, req('cmd_1'));
      const actions = paxos.onClientRequest(node, req('cmd_2'));
      expect(actions.length).toBe(0); // already proposing
      expect((node.meta.commandQueue as ClientRequest[]).length).toBe(2);
    });

    it('any node can accept client requests', () => {
      const node = makeNode('node_2');
      expect(paxos.canAcceptClientRequest(node)).toBe(true);
    });
  });

  describe('Prepare/Promise phase', () => {
    it('acceptor promises on prepare with higher proposal number', () => {
      const acceptor = makeNode('node_1');
      const actions = paxos.onMessage(acceptor, msg({
        type: 'prepare', from: 'node_0', to: 'node_1', term: 3,
        payload: { proposalNumber: 3, slot: 0 },
      }));
      expect(acceptor.meta.minProposal).toBe(3);
      const promise = actions.find(a => a.message?.type === 'promise');
      expect(promise).toBeDefined();
      expect(promise!.message!.payload.proposalNumber).toBe(3);
    });

    it('acceptor NACKs on prepare with lower proposal number', () => {
      const acceptor = makeNode('node_1');
      (acceptor.meta.slotStates as Record<number, {
        minProposal: number;
        acceptedProposal: number;
        acceptedValue: ClientRequest | null;
      }>)[0] = {
        minProposal: 10,
        acceptedProposal: -1,
        acceptedValue: null,
      };
      const actions = paxos.onMessage(acceptor, msg({
        type: 'prepare', from: 'node_0', to: 'node_1', term: 3,
        payload: { proposalNumber: 3, slot: 0 },
      }));
      const nack = actions.find(a => a.message?.type === 'nack');
      expect(nack).toBeDefined();
    });

    it('promise includes previously accepted value', () => {
      const acceptor = makeNode('node_1');
      (acceptor.meta.slotStates as Record<number, {
        minProposal: number;
        acceptedProposal: number;
        acceptedValue: ClientRequest | null;
      }>)[0] = {
        minProposal: 0,
        acceptedProposal: 2,
        acceptedValue: req('old_cmd'),
      };
      const actions = paxos.onMessage(acceptor, msg({
        type: 'prepare', from: 'node_0', to: 'node_1', term: 5,
        payload: { proposalNumber: 5, slot: 0 },
      }));
      const promise = actions.find(a => a.message?.type === 'promise');
      expect(promise!.message!.payload.acceptedProposal).toBe(2);
      expect((promise!.message!.payload.acceptedValue as ClientRequest).command).toBe('old_cmd');
    });
  });

  describe('full consensus flow', () => {
    it('commits value through Prepare→Promise→Accept→Accepted', () => {
      const proposer = makeNode('node_0');
      paxos.onClientRequest(proposer, req('cmd_1'));
      expect(proposer.meta.proposalPhase).toBe('prepare');

      const proposalNumber = proposer.meta.proposalNumber as number;

      // Receive promise from node_1 (self already promised, so 2/3 = majority)
      const actionsAfterPromise = paxos.onMessage(proposer, msg({
        type: 'promise', from: 'node_1', to: 'node_0', term: proposalNumber,
        payload: { proposalNumber, slot: 0, acceptedProposal: -1, acceptedValue: null },
      }));
      expect(proposer.meta.proposalPhase).toBe('accept');
      const accepts = actionsAfterPromise.filter(a => a.message?.type === 'accept');
      expect(accepts.length).toBe(2); // to both peers

      // Receive accepted from node_1 (self already accepted, so 2/3 = majority)
      const actionsAfterAccept = paxos.onMessage(proposer, msg({
        type: 'accepted', from: 'node_1', to: 'node_0', term: proposalNumber,
        payload: { proposalNumber, slot: 0, value: req('cmd_1') },
      }));
      expect(proposer.meta.isProposing).toBe(false);
      expect(proposer.log.length).toBe(1);
      expect(proposer.log[0].command).toBe('cmd_1');
      expect(proposer.log[0].committed).toBe(true);
      // Should emit commit_entry and learn messages
      expect(actionsAfterAccept.some(a => a.type === 'commit_entry')).toBe(true);
      const learns = actionsAfterAccept.filter(a => a.message?.type === 'learn');
      expect(learns.length).toBe(2);
    });
  });

  describe('Learn phase', () => {
    it('adds committed entry on learn', () => {
      const node = makeNode('node_1');
      paxos.onMessage(node, msg({
        type: 'learn', from: 'node_0', to: 'node_1', term: 3,
        payload: { slot: 0, value: req('cmd_1'), proposalNumber: 3, commitIndex: 0 },
      }));
      expect(node.log.length).toBe(1);
      expect(node.log[0].command).toBe('cmd_1');
      expect(node.log[0].committed).toBe(true);
    });

    it('does not duplicate already committed value', () => {
      const node = makeNode('node_1');
      node.log.push({
        entryId: 'entry_cmd_1',
        requestId: 'req_cmd_1',
        term: 3,
        index: 0,
        command: 'cmd_1',
        committed: true,
      });
      paxos.onMessage(node, msg({
        type: 'learn', from: 'node_0', to: 'node_1', term: 3,
        payload: { slot: 0, value: req('cmd_1'), proposalNumber: 3, commitIndex: 0 },
      }));
      expect(node.log.length).toBe(1); // still 1
    });

    it('resets acceptor state after learn', () => {
      const node = makeNode('node_1');
      node.meta.acceptedProposal = 5;
      node.meta.acceptedValue = req('cmd_1');
      paxos.onMessage(node, msg({
        type: 'learn', from: 'node_0', to: 'node_1', term: 5,
        payload: { slot: 0, value: req('cmd_1'), proposalNumber: 5, commitIndex: 0 },
      }));
      expect(node.meta.acceptedProposal).toBe(-1);
      expect(node.meta.acceptedValue).toBeNull();
    });

    it('stops proposing if learned value matches pending value', () => {
      const node = makeNode('node_0');
      paxos.onClientRequest(node, req('cmd_1'));
      expect(node.meta.isProposing).toBe(true);
      paxos.onMessage(node, msg({
        type: 'learn', from: 'node_1', to: 'node_0', term: 10,
        payload: { slot: 0, value: req('cmd_1'), proposalNumber: 10, commitIndex: 0 },
      }));
      expect(node.meta.isProposing).toBe(false);
      expect(node.meta.pendingValue).toBeNull();
    });
  });

  describe('NACK handling', () => {
    it('backs off on NACK', () => {
      const node = makeNode('node_0');
      paxos.onClientRequest(node, req('cmd_1'));
      const proposalNumber = node.meta.proposalNumber as number;
      const actions = paxos.onMessage(node, msg({
        type: 'nack', from: 'node_1', to: 'node_0', term: 100,
        payload: { proposalNumber, slot: 0, highestSeen: 100 },
      }));
      expect(node.meta.isProposing).toBe(false);
      expect(node.role).toBe('acceptor');
      const timeout = actions.find(a => a.type === 'set_timeout');
      expect(timeout).toBeDefined();
    });

    it('ignores stale NACKs from old proposals', () => {
      const node = makeNode('node_0');
      paxos.onClientRequest(node, req('cmd_1'));
      const actions = paxos.onMessage(node, msg({
        type: 'nack', from: 'node_1', to: 'node_0', term: 100,
        payload: { proposalNumber: 999, slot: 0, highestSeen: 100 }, // wrong proposal number
      }));
      expect(actions.length).toBe(0);
      expect(node.meta.isProposing).toBe(true); // still proposing
    });

    it('bumps seqNum above highest seen', () => {
      const node = makeNode('node_0');
      paxos.onClientRequest(node, req('cmd_1'));
      const proposalNumber = node.meta.proposalNumber as number;
      paxos.onMessage(node, msg({
        type: 'nack', from: 'node_1', to: 'node_0', term: 100,
        payload: { proposalNumber, slot: 0, highestSeen: 100 },
      }));
      expect((node.meta.seqNum as number)).toBeGreaterThan(1);
    });
  });

  describe('late promise handling', () => {
    it('ignores promises when already in accept phase', () => {
      const proposer = makeNode('node_0');
      paxos.onClientRequest(proposer, req('cmd_1'));
      const pn = proposer.meta.proposalNumber as number;

      // Get to accept phase
      paxos.onMessage(proposer, msg({
        type: 'promise', from: 'node_1', to: 'node_0', term: pn,
        payload: { proposalNumber: pn, slot: 0, acceptedProposal: -1, acceptedValue: null },
      }));
      expect(proposer.meta.proposalPhase).toBe('accept');

      // Late promise from node_2 — should be ignored
      const actions = paxos.onMessage(proposer, msg({
        type: 'promise', from: 'node_2', to: 'node_0', term: pn,
        payload: { proposalNumber: pn, slot: 0, acceptedProposal: -1, acceptedValue: null },
      }));
      expect(actions.length).toBe(0);
    });
  });

  describe('value adoption from acceptors', () => {
    it('adopts previously accepted value from promise', () => {
      const proposer = makeNode('node_0');
      paxos.onClientRequest(proposer, req('my_cmd'));
      const pn = proposer.meta.proposalNumber as number;

      // node_1 already accepted 'old_cmd' with proposal 2
      const actions = paxos.onMessage(proposer, msg({
        type: 'promise', from: 'node_1', to: 'node_0', term: pn,
        payload: { proposalNumber: pn, slot: 0, acceptedProposal: 2, acceptedValue: req('old_cmd') },
      }));

      // Should propose 'old_cmd' instead of 'my_cmd'
      const accepts = actions.filter(a => a.message?.type === 'accept');
      expect(accepts.length).toBeGreaterThan(0);
      expect((accepts[0].message!.payload.value as ClientRequest).command).toBe('old_cmd');
    });

    it('does not adopt already committed values', () => {
      const proposer = makeNode('node_0');
      // Pre-commit 'old_cmd'
      proposer.log.push({
        entryId: 'entry_old_cmd',
        requestId: 'req_old_cmd',
        term: 2,
        index: 0,
        command: 'old_cmd',
        committed: true,
      });
      paxos.onClientRequest(proposer, req('new_cmd'));
      const pn = proposer.meta.proposalNumber as number;

      // node_1 has stale accepted state with 'old_cmd'
      const actions = paxos.onMessage(proposer, msg({
        type: 'promise', from: 'node_1', to: 'node_0', term: pn,
        payload: { proposalNumber: pn, slot: 1, acceptedProposal: 2, acceptedValue: req('old_cmd') },
      }));

      // Should propose 'new_cmd', not 'old_cmd'
      const accepts = actions.filter(a => a.message?.type === 'accept');
      expect(accepts.length).toBeGreaterThan(0);
      expect((accepts[0].message!.payload.value as ClientRequest).command).toBe('new_cmd');
    });
  });

  describe('acceptor state reset after commit', () => {
    it('resets acceptedProposal/Value after handleAccepted majority', () => {
      const proposer = makeNode('node_0');
      paxos.onClientRequest(proposer, req('cmd_1'));
      const pn = proposer.meta.proposalNumber as number;

      // Promise phase
      paxos.onMessage(proposer, msg({
        type: 'promise', from: 'node_1', to: 'node_0', term: pn,
        payload: { proposalNumber: pn, slot: 0, acceptedProposal: -1, acceptedValue: null },
      }));

      // Accepted phase
      paxos.onMessage(proposer, msg({
        type: 'accepted', from: 'node_1', to: 'node_0', term: pn,
        payload: { proposalNumber: pn, slot: 0, value: req('cmd_1') },
      }));

      expect(proposer.meta.acceptedProposal).toBe(-1);
      expect(proposer.meta.acceptedValue).toBeNull();
    });
  });

  describe('proposal timeout', () => {
    it('retries proposal on election timeout', () => {
      const node = makeNode('node_0');
      paxos.onClientRequest(node, req('cmd_1'));
      const oldPn = node.meta.proposalNumber as number;
      // Simulate timeout (no promises received)
      const actions = paxos.onTimeout(node, 'election');
      expect(actions.length).toBeGreaterThan(0);
      expect(node.meta.proposalNumber).toBeGreaterThan(oldPn);
    });

    it('does nothing on timeout with empty queue', () => {
      const node = makeNode('node_0');
      const actions = paxos.onTimeout(node, 'election');
      expect(actions.length).toBe(0);
    });
  });

  describe('recovery', () => {
    it('resets proposer state', () => {
      const node = makeNode('node_0');
      paxos.onClientRequest(node, req('cmd_1'));
      expect(node.meta.isProposing).toBe(true);
      paxos.onRecovery(node, makeConfig());
      expect(node.role).toBe('acceptor');
      expect(node.meta.isProposing).toBe(false);
      expect(node.meta.proposalPhase).toBeNull();
    });
  });

  describe('getKnownLeader', () => {
    it('always returns null (Paxos has no leader)', () => {
      const node = makeNode('node_0');
      expect(paxos.getKnownLeader(node)).toBeNull();
    });
  });

  describe('sequential commits', () => {
    it('processes multiple commands in queue', () => {
      const proposer = makeNode('node_0');
      paxos.onClientRequest(proposer, req('cmd_1'));
      paxos.onClientRequest(proposer, req('cmd_2'));
      expect((proposer.meta.commandQueue as ClientRequest[]).length).toBe(2);

      const pn = proposer.meta.proposalNumber as number;

      // Complete first proposal
      paxos.onMessage(proposer, msg({
        type: 'promise', from: 'node_1', to: 'node_0', term: pn,
        payload: { proposalNumber: pn, slot: 0, acceptedProposal: -1, acceptedValue: null },
      }));
      paxos.onMessage(proposer, msg({
        type: 'accepted', from: 'node_1', to: 'node_0', term: pn,
        payload: { proposalNumber: pn, slot: 0, value: req('cmd_1') },
      }));

      // After committing cmd_1, should auto-start proposing cmd_2
      expect(proposer.meta.isProposing).toBe(true);
      expect((proposer.meta.pendingValue as ClientRequest | null)?.command).toBe('cmd_2');
      expect(proposer.meta.currentSlot).toBe(1);
      expect(proposer.log.length).toBe(1);
      expect(proposer.log[0].command).toBe('cmd_1');
    });
  });
});
