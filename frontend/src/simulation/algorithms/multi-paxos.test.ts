import { describe, it, expect } from 'vitest';
import { MultiPaxosAlgorithm } from './multi-paxos';
import { ClusterConfig, NodeState, Message, ClientRequest } from '../types';

const multiPaxos = new MultiPaxosAlgorithm();

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
  return multiPaxos.getInitialState(id, config);
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

describe('MultiPaxosAlgorithm', () => {
  it('uses an explicit slot for the first proposal after election', () => {
    const node = makeNode('node_0');

    const electionActions = multiPaxos.onTimeout(node, 'election');
    const electionPrepares = electionActions.filter(action => action.message?.type === 'prepare');
    expect(electionPrepares).toHaveLength(2);
    expect(electionPrepares[0].message!.payload.slot).toBe(0);

    multiPaxos.onMessage(node, msg({
      type: 'promise',
      from: 'node_1',
      to: 'node_0',
      term: node.meta.proposalNumber as number,
      payload: {
        proposalNumber: node.meta.proposalNumber,
        slot: 0,
        acceptedProposal: -1,
        acceptedValue: null,
      },
    }));

    expect(node.role).toBe('leader');
    expect(node.meta.leaderEstablished).toBe(false);

    const actions = multiPaxos.onClientRequest(node, req('cmd_1'));
    const prepares = actions.filter(action => action.message?.type === 'prepare');
    expect(prepares).toHaveLength(2);
    expect(prepares[0].message!.payload.slot).toBe(0);
    expect(node.meta.currentSlot).toBe(0);
  });

  it('commits identical commands into distinct slots under a stable leader', () => {
    const leader = makeNode('node_0');
    leader.role = 'leader';
    leader.currentTerm = 7;
    leader.meta.knownLeader = 'node_0';
    leader.meta.leaderBallot = 7;
    leader.meta.leaderEstablished = true;

    const firstRequest = req('set x=1', 'a');
    const secondRequest = req('set x=1', 'b');

    const firstActions = multiPaxos.onClientRequest(leader, firstRequest);
    const firstAccepts = firstActions.filter(action => action.message?.type === 'accept');
    expect(firstAccepts).toHaveLength(2);
    expect(firstAccepts[0].message!.payload.slot).toBe(0);

    multiPaxos.onMessage(leader, msg({
      type: 'accepted',
      from: 'node_1',
      to: 'node_0',
      term: 7,
      payload: { proposalNumber: 7, slot: 0, value: firstRequest },
    }));

    const secondActions = multiPaxos.onClientRequest(leader, secondRequest);
    const secondAccepts = secondActions.filter(action => action.message?.type === 'accept');
    expect(secondAccepts).toHaveLength(2);
    expect(secondAccepts[0].message!.payload.slot).toBe(1);

    multiPaxos.onMessage(leader, msg({
      type: 'accepted',
      from: 'node_1',
      to: 'node_0',
      term: 7,
      payload: { proposalNumber: 7, slot: 1, value: secondRequest },
    }));

    expect(leader.log).toHaveLength(2);
    expect(leader.log.map(entry => entry.index)).toEqual([0, 1]);
    expect(leader.log.map(entry => entry.command)).toEqual(['set x=1', 'set x=1']);
    expect(leader.log.map(entry => entry.entryId)).toEqual(['entry_a', 'entry_b']);
  });

  it('applies learned values at the provided slot', () => {
    const follower = makeNode('node_1');

    multiPaxos.onMessage(follower, msg({
      type: 'learn',
      from: 'node_0',
      to: 'node_1',
      term: 9,
      payload: {
        slot: 3,
        value: req('cmd_3'),
        proposalNumber: 9,
        commitIndex: 3,
      },
    }));

    expect(follower.log).toHaveLength(1);
    expect(follower.log[0].index).toBe(3);
    expect(follower.commitIndex).toBe(3);
    expect(follower.meta.nextProposalSlot).toBe(4);
  });
});
