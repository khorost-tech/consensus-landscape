import { describe, it, expect } from 'vitest';
import { ZabAlgorithm } from './zab';
import { ClusterConfig, NodeState, Message, ClientRequest } from '../types';

const zab = new ZabAlgorithm();

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
  return zab.getInitialState(id, config);
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

describe('ZabAlgorithm', () => {
  it('includes explicit zxid and index in proposals', () => {
    const leader = makeNode('node_0');
    leader.role = 'leading';
    leader.meta.phase = 'broadcast';
    leader.meta.epoch = 3;
    leader.currentTerm = 3;

    const actions = zab.onClientRequest(leader, req('cmd_1'));
    const proposal = actions.find(action => action.message?.type === 'zab_proposal');

    expect(proposal).toBeDefined();
    expect(proposal!.message!.payload.zxidKey).toBe('3:1');
    expect(proposal!.message!.payload.index).toBe(0);
    expect(leader.log[0].zxid).toBe('3:1');
  });

  it('commits the exact matching entry on zab_commit', () => {
    const follower = makeNode('node_1');
    follower.role = 'following';
    follower.meta.phase = 'broadcast';
    follower.log.push(
      {
        entryId: 'entry_a',
        requestId: 'req_a',
        zxid: '2:1',
        term: 2,
        index: 0,
        command: 'set x=1',
        committed: false,
      },
      {
        entryId: 'entry_b',
        requestId: 'req_b',
        zxid: '2:2',
        term: 2,
        index: 1,
        command: 'set x=1',
        committed: false,
      },
    );

    zab.onMessage(follower, msg({
      type: 'zab_commit',
      from: 'node_0',
      to: 'node_1',
      term: 2,
      payload: {
        zxidKey: '2:2',
        epoch: 2,
        counter: 2,
        index: 1,
        requestId: 'req_b',
        entryId: 'entry_b',
      },
    }));

    expect(follower.log[0].committed).toBe(false);
    expect(follower.log[1].committed).toBe(true);
    expect(follower.commitIndex).toBe(1);
  });

  it('applies pending commit metadata when proposal arrives later', () => {
    const follower = makeNode('node_1');
    follower.role = 'following';
    follower.meta.phase = 'broadcast';

    zab.onMessage(follower, msg({
      type: 'zab_commit',
      from: 'node_0',
      to: 'node_1',
      term: 4,
      payload: {
        zxidKey: '4:3',
        epoch: 4,
        counter: 3,
        index: 5,
        requestId: 'req_late',
        entryId: 'entry_late',
      },
    }));

    const actions = zab.onMessage(follower, msg({
      type: 'zab_proposal',
      from: 'node_0',
      to: 'node_1',
      term: 4,
      payload: {
        epoch: 4,
        counter: 3,
        index: 5,
        value: 'set y=2',
        requestId: 'req_late',
        entryId: 'entry_late',
        zxidKey: '4:3',
      },
    }));

    expect(actions[0].message?.type).toBe('zab_ack');
    expect(follower.log).toHaveLength(1);
    expect(follower.log[0].zxid).toBe('4:3');
    expect(follower.log[0].committed).toBe(true);
    expect(follower.log[0].index).toBe(5);
    expect(follower.commitIndex).toBe(5);
  });
});
