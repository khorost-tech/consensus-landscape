import { describe, it, expect } from 'vitest';
import { RaftAlgorithm } from './raft';
import { ClusterConfig, NodeState, Message, ClientRequest } from '../types';

const raft = new RaftAlgorithm();

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
  return raft.getInitialState(id, config);
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

describe('RaftAlgorithm', () => {
  describe('getInitialState', () => {
    it('starts as follower', () => {
      const node = makeNode('node_0');
      expect(node.role).toBe('follower');
      expect(node.currentTerm).toBe(0);
      expect(node.votedFor).toBeNull();
      expect(node.log).toHaveLength(0);
    });

    it('sets peers correctly', () => {
      const node = makeNode('node_1', makeConfig(3));
      const peers = node.meta.peers as string[];
      expect(peers).toContain('node_0');
      expect(peers).toContain('node_2');
      expect(peers).not.toContain('node_1');
    });
  });

  describe('election timeout', () => {
    it('starts election on timeout', () => {
      const node = makeNode('node_0');
      const actions = raft.onTimeout(node, 'election');
      expect(node.role).toBe('candidate');
      expect(node.currentTerm).toBe(1);
      expect(node.votedFor).toBe('node_0');
      expect(node.votesReceived.has('node_0')).toBe(true);
      // Should send request_vote to peers
      const voteRequests = actions.filter(a => a.message?.type === 'request_vote');
      expect(voteRequests.length).toBe(2); // 2 peers in 3-node cluster
    });

    it('sets election timeout after starting election', () => {
      const node = makeNode('node_0');
      const actions = raft.onTimeout(node, 'election');
      const timeout = actions.find(a => a.type === 'set_timeout');
      expect(timeout).toBeDefined();
      expect(timeout!.timeout!.type).toBe('election');
    });
  });

  describe('vote handling', () => {
    it('grants vote to first candidate with up-to-date log', () => {
      const node = makeNode('node_1');
      const actions = raft.onMessage(node, msg({
        type: 'request_vote', from: 'node_0', to: 'node_1', term: 1,
        payload: { candidateId: 'node_0', lastLogIndex: -1, lastLogTerm: 0 },
      }));
      expect(node.votedFor).toBe('node_0');
      const resp = actions.find(a => a.message?.type === 'request_vote_response');
      expect(resp!.message!.payload.voteGranted).toBe(true);
    });

    it('rejects vote if already voted for another', () => {
      const node = makeNode('node_1');
      node.votedFor = 'node_2';
      node.currentTerm = 1;
      const actions = raft.onMessage(node, msg({
        type: 'request_vote', from: 'node_0', to: 'node_1', term: 1,
        payload: { candidateId: 'node_0', lastLogIndex: -1, lastLogTerm: 0 },
      }));
      const resp = actions.find(a => a.message?.type === 'request_vote_response');
      expect(resp!.message!.payload.voteGranted).toBe(false);
    });

    it('becomes leader on majority votes', () => {
      const node = makeNode('node_0');
      // Start election
      raft.onTimeout(node, 'election');
      expect(node.role).toBe('candidate');
      // Receive vote from node_1 (already self-voted, so 2/3 = majority)
      raft.onMessage(node, msg({
        type: 'request_vote_response', from: 'node_1', to: 'node_0', term: 1,
        payload: { voteGranted: true },
      }));
      expect(node.role).toBe('leader');
    });

    it('does not become leader without majority', () => {
      const node = makeNode('node_0', makeConfig(5));
      raft.onTimeout(node, 'election');
      // Only 1 vote from peer (self + 1 = 2, need 3)
      raft.onMessage(node, msg({
        type: 'request_vote_response', from: 'node_1', to: 'node_0', term: 1,
        payload: { voteGranted: true },
      }));
      expect(node.role).toBe('candidate');
    });
  });

  describe('log replication', () => {
    it('leader appends client request to log', () => {
      const node = makeNode('node_0');
      raft.onTimeout(node, 'election');
      raft.onMessage(node, msg({
        type: 'request_vote_response', from: 'node_1', to: 'node_0', term: 1,
        payload: { voteGranted: true },
      }));
      expect(node.role).toBe('leader');
      const actions = raft.onClientRequest(node, req('set x=1'));
      expect(node.log.length).toBe(1);
      expect(node.log[0].command).toBe('set x=1');
      // Should send append_entries to peers
      const appends = actions.filter(a => a.message?.type === 'append_entries');
      expect(appends.length).toBe(2);
    });

    it('non-leader redirects client request', () => {
      const node = makeNode('node_1');
      const actions = raft.onClientRequest(node, req('cmd'));
      const redirect = actions.find(a => a.message?.type === 'client_response');
      expect(redirect!.message!.payload.redirect).toBe(true);
    });

    it('follower appends entries from leader', () => {
      const follower = makeNode('node_1');
      follower.currentTerm = 1;
      const entry = {
        entryId: 'entry_cmd_1',
        requestId: 'req_cmd_1',
        term: 1,
        index: 0,
        command: 'cmd_1',
        committed: false,
      };
      raft.onMessage(follower, msg({
        type: 'append_entries', from: 'node_0', to: 'node_1', term: 1,
        payload: {
          leaderId: 'node_0', prevLogIndex: -1, prevLogTerm: 0,
          entries: [entry], leaderCommit: -1,
        },
      }));
      expect(follower.log.length).toBe(1);
      expect(follower.log[0].command).toBe('cmd_1');
    });

    it('commits entry when majority replicates', () => {
      const leader = makeNode('node_0');
      raft.onTimeout(leader, 'election');
      raft.onMessage(leader, msg({
        type: 'request_vote_response', from: 'node_1', to: 'node_0', term: 1,
        payload: { voteGranted: true },
      }));
      raft.onClientRequest(leader, req('cmd_1'));
      // Follower responds with success
      const actions = raft.onMessage(leader, msg({
        type: 'append_entries_response', from: 'node_1', to: 'node_0', term: 1,
        payload: { success: true, matchIndex: 0 },
      }));
      expect(leader.commitIndex).toBe(0);
      expect(leader.log[0].committed).toBe(true);
      expect(actions.some(a => a.type === 'commit_entry')).toBe(true);
    });
  });

  describe('term handling', () => {
    it('steps down on higher term message', () => {
      const node = makeNode('node_0');
      raft.onTimeout(node, 'election');
      expect(node.role).toBe('candidate');
      raft.onMessage(node, msg({
        type: 'append_entries', from: 'node_1', to: 'node_0', term: 5,
        payload: { leaderId: 'node_1', prevLogIndex: -1, prevLogTerm: 0, entries: [], leaderCommit: -1 },
      }));
      expect(node.role).toBe('follower');
      expect(node.currentTerm).toBe(5);
    });
  });

  describe('heartbeat', () => {
    it('leader sends heartbeats on heartbeat timeout', () => {
      const node = makeNode('node_0');
      raft.onTimeout(node, 'election');
      raft.onMessage(node, msg({
        type: 'request_vote_response', from: 'node_1', to: 'node_0', term: 1,
        payload: { voteGranted: true },
      }));
      expect(node.role).toBe('leader');
      const actions = raft.onTimeout(node, 'heartbeat');
      const heartbeats = actions.filter(a => a.message?.type === 'append_entries');
      expect(heartbeats.length).toBe(2); // to both peers
    });

    it('follower resets election timer on heartbeat', () => {
      const node = makeNode('node_1');
      node.currentTerm = 1;
      const actions = raft.onMessage(node, msg({
        type: 'append_entries', from: 'node_0', to: 'node_1', term: 1,
        payload: { leaderId: 'node_0', prevLogIndex: -1, prevLogTerm: 0, entries: [], leaderCommit: -1 },
      }));
      const timeout = actions.find(a => a.type === 'set_timeout' && a.timeout?.type === 'election');
      expect(timeout).toBeDefined();
    });

    it('sends install_snapshot when follower is behind trimmed prefix', () => {
      const node = makeNode('node_0');
      node.role = 'leader';
      node.currentTerm = 3;
      node.logBaseIndex = 5;
      node.logBaseTerm = 2;
      node.log = [
        { ...req('cmd_5'), term: 3, index: 5, committed: true },
        { ...req('cmd_6'), term: 3, index: 6, committed: false },
      ];
      node.nextIndex.set('node_1', 2);

      const actions = raft.onTimeout(node, 'heartbeat');
      const snapshot = actions.find(a => a.message?.type === 'install_snapshot' && a.message.to === 'node_1');

      expect(snapshot).toBeDefined();
      expect(snapshot!.message!.payload.lastIncludedIndex).toBe(4);
      expect(snapshot!.message!.payload.lastIncludedTerm).toBe(2);
    });

    it('follower installs snapshot and acknowledges it', () => {
      const node = makeNode('node_1');
      node.currentTerm = 3;
      node.log = [
        { ...req('old_0'), term: 1, index: 0, committed: true },
        { ...req('old_1'), term: 2, index: 1, committed: false },
      ];

      const actions = raft.onMessage(node, msg({
        type: 'install_snapshot',
        from: 'node_0',
        to: 'node_1',
        term: 3,
        payload: {
          leaderId: 'node_0',
          lastIncludedIndex: 4,
          lastIncludedTerm: 2,
          leaderCommit: 4,
        },
      }));

      expect(node.logBaseIndex).toBe(5);
      expect(node.logBaseTerm).toBe(2);
      expect(node.commitIndex).toBe(4);
      expect(node.lastApplied).toBe(4);
      expect(actions.some(a => a.message?.type === 'install_snapshot_response')).toBe(true);
    });

    it('leader resumes append_entries after snapshot acknowledgement', () => {
      const node = makeNode('node_0');
      node.role = 'leader';
      node.currentTerm = 3;
      node.logBaseIndex = 5;
      node.logBaseTerm = 2;
      node.log = [
        { ...req('cmd_5'), term: 3, index: 5, committed: true },
        { ...req('cmd_6'), term: 3, index: 6, committed: false },
      ];

      const actions = raft.onMessage(node, msg({
        type: 'install_snapshot_response',
        from: 'node_1',
        to: 'node_0',
        term: 3,
        payload: { lastIncludedIndex: 4 },
      }));

      const append = actions.find(a => a.message?.type === 'append_entries');
      expect(append).toBeDefined();
      expect(append!.message!.payload.prevLogIndex).toBe(4);
      expect(append!.message!.payload.prevLogTerm).toBe(2);
    });
  });

  describe('recovery', () => {
    it('resets to follower on recovery', () => {
      const node = makeNode('node_0');
      raft.onTimeout(node, 'election');
      expect(node.role).toBe('candidate');
      const actions = raft.onRecovery(node, makeConfig());
      expect(node.role).toBe('follower');
      expect(node.votedFor).toBeNull();
      const timeout = actions.find(a => a.type === 'set_timeout');
      expect(timeout).toBeDefined();
    });
  });

  describe('canAcceptClientRequest / getKnownLeader', () => {
    it('only leader can accept', () => {
      const follower = makeNode('node_0');
      expect(raft.canAcceptClientRequest(follower)).toBe(false);
    });

    it('returns known leader', () => {
      const node = makeNode('node_0');
      node.meta.knownLeader = 'node_1';
      expect(raft.getKnownLeader(node)).toBe('node_1');
    });
  });
});
