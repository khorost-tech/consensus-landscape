import { describe, it, expect } from 'vitest';
import { EPaxosAlgorithm } from './epaxos';
import { ClusterConfig, NodeState, Message, ClientRequest } from '../types';

const epaxos = new EPaxosAlgorithm();

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
  return epaxos.getInitialState(id, config);
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

describe('EPaxosAlgorithm', () => {
  it('commits on fast path and stores instanceKey in the log', () => {
    const node = makeNode('node_0');

    epaxos.onClientRequest(node, req('cmd_1'));

    const actions = epaxos.onMessage(node, msg({
      type: 'ep_preaccept_ok',
      from: 'node_1',
      to: 'node_0',
      term: 0,
      payload: {
        instanceKey: 'node_0:1',
        seq: 1,
        deps: [],
        depsMatch: true,
      },
    }));

    expect(actions.some(action => action.type === 'commit_entry')).toBe(true);
    expect(node.log).toHaveLength(1);
    expect(node.log[0].instanceKey).toBe('node_0:1');
    expect(node.log[0].committed).toBe(true);
    expect(node.meta.activeInstance).toBeNull();
  });

  it('keeps fast-path seq unchanged when there is no local conflict', () => {
    const node = makeNode('node_1');

    const actions = epaxos.onMessage(node, msg({
      type: 'ep_preaccept',
      from: 'node_0',
      to: 'node_1',
      term: 0,
      payload: {
        instanceKey: 'node_0:1',
        command: 'cmd_1',
        seq: 1,
        deps: [],
        requestId: 'req_cmd_1',
        entryId: 'entry_cmd_1',
      },
    }));

    expect(actions[0].message?.type).toBe('ep_preaccept_ok');
    expect(actions[0].message?.payload.seq).toBe(1);
    expect(actions[0].message?.payload.deps).toEqual([]);
  });

  it('switches to slow path when dependencies do not match', () => {
    const node = makeNode('node_0');

    epaxos.onClientRequest(node, req('cmd_1'));

    const actions = epaxos.onMessage(node, msg({
      type: 'ep_preaccept_ok',
      from: 'node_1',
      to: 'node_0',
      term: 0,
      payload: {
        instanceKey: 'node_0:1',
        seq: 2,
        deps: ['node_2:7'],
        depsMatch: false,
      },
    }));

    const accepts = actions.filter(action => action.message?.type === 'ep_accept');
    expect(accepts).toHaveLength(2);

    const instance = (node.meta.instances as Record<string, {
      status: string;
      seq: number;
      deps: string[];
    }>)['node_0:1'];
    expect(instance.status).toBe('accepted');
    expect(instance.seq).toBe(2);
    expect(instance.deps).toContain('node_2:7');
  });

  it('does not downgrade a committed instance when late preaccept arrives', () => {
    const node = makeNode('node_1');

    epaxos.onMessage(node, msg({
      type: 'ep_commit',
      from: 'node_0',
      to: 'node_1',
      term: 0,
      payload: {
        instanceKey: 'node_0:3',
        command: 'set x=1',
        seq: 4,
        deps: ['node_2:1'],
        index: 5,
        requestId: 'req_commit_first',
        entryId: 'entry_commit_first',
      },
    }));

    epaxos.onMessage(node, msg({
      type: 'ep_preaccept',
      from: 'node_0',
      to: 'node_1',
      term: 0,
      payload: {
        instanceKey: 'node_0:3',
        command: 'set x=1',
        seq: 3,
        deps: [],
        requestId: 'req_commit_first',
        entryId: 'entry_commit_first',
      },
    }));

    const instance = (node.meta.instances as Record<string, {
      status: string;
      seq: number;
      deps: string[];
    }>)['node_0:3'];
    expect(instance.status).toBe('committed');
    expect(instance.seq).toBe(4);
    expect(instance.deps).toContain('node_2:1');
    expect(node.log).toHaveLength(0);
  });

  it('materializes committed instances only after their dependencies are materialized', () => {
    const node = makeNode('node_1');

    epaxos.onMessage(node, msg({
      type: 'ep_commit',
      from: 'node_0',
      to: 'node_1',
      term: 0,
      payload: {
        instanceKey: 'node_0:2',
        command: 'after_dep',
        seq: 2,
        deps: ['node_0:1'],
        requestId: 'req_after_dep',
        entryId: 'entry_after_dep',
      },
    }));

    expect(node.log).toHaveLength(0);

    epaxos.onMessage(node, msg({
      type: 'ep_commit',
      from: 'node_0',
      to: 'node_1',
      term: 0,
      payload: {
        instanceKey: 'node_0:1',
        command: 'dep',
        seq: 1,
        deps: [],
        requestId: 'req_dep',
        entryId: 'entry_dep',
      },
    }));

    expect(node.log).toHaveLength(2);
    expect(node.log.map(entry => entry.instanceKey)).toEqual(['node_0:1', 'node_0:2']);
    expect(node.log.map(entry => entry.command)).toEqual(['dep', 'after_dep']);
  });

  it('materializes a committed dependency cycle as one execution batch', () => {
    const node = makeNode('node_1');

    epaxos.onMessage(node, msg({
      type: 'ep_commit',
      from: 'node_0',
      to: 'node_1',
      term: 0,
      payload: {
        instanceKey: 'node_0:1',
        command: 'cycle_a',
        seq: 1,
        deps: ['node_0:2'],
        requestId: 'req_cycle_a',
        entryId: 'entry_cycle_a',
      },
    }));

    expect(node.log).toHaveLength(0);

    epaxos.onMessage(node, msg({
      type: 'ep_commit',
      from: 'node_0',
      to: 'node_1',
      term: 0,
      payload: {
        instanceKey: 'node_0:2',
        command: 'cycle_b',
        seq: 2,
        deps: ['node_0:1'],
        requestId: 'req_cycle_b',
        entryId: 'entry_cycle_b',
      },
    }));

    expect(node.log).toHaveLength(2);
    expect(node.log.map(entry => entry.instanceKey)).toEqual(['node_0:1', 'node_0:2']);
    expect(node.log.map(entry => entry.command)).toEqual(['cycle_a', 'cycle_b']);
  });

  it('keeps a committed cycle blocked until its external dependency is materialized', () => {
    const node = makeNode('node_1');

    epaxos.onMessage(node, msg({
      type: 'ep_commit',
      from: 'node_0',
      to: 'node_1',
      term: 0,
      payload: {
        instanceKey: 'node_0:2',
        command: 'cycle_a',
        seq: 2,
        deps: ['node_0:3'],
        requestId: 'req_cycle_a_blocked',
        entryId: 'entry_cycle_a_blocked',
      },
    }));

    epaxos.onMessage(node, msg({
      type: 'ep_commit',
      from: 'node_0',
      to: 'node_1',
      term: 0,
      payload: {
        instanceKey: 'node_0:3',
        command: 'cycle_b',
        seq: 3,
        deps: ['node_0:2', 'node_0:1'],
        requestId: 'req_cycle_b_blocked',
        entryId: 'entry_cycle_b_blocked',
      },
    }));

    expect(node.log).toHaveLength(0);

    epaxos.onMessage(node, msg({
      type: 'ep_commit',
      from: 'node_0',
      to: 'node_1',
      term: 0,
      payload: {
        instanceKey: 'node_0:1',
        command: 'external_dep',
        seq: 1,
        deps: [],
        requestId: 'req_external_dep',
        entryId: 'entry_external_dep',
      },
    }));

    expect(node.log).toHaveLength(3);
    expect(node.log.map(entry => entry.instanceKey)).toEqual(['node_0:1', 'node_0:2', 'node_0:3']);
  });
});
