import { describe, it, expect } from 'vitest';
import { SimulationEngine } from './engine';
import { RaftAlgorithm } from './algorithms/raft';
import { PaxosAlgorithm } from './algorithms/paxos';
import { MultiPaxosAlgorithm } from './algorithms/multi-paxos';
import { ZabAlgorithm } from './algorithms/zab';
import { EPaxosAlgorithm } from './algorithms/epaxos';
import { ClusterConfig } from './types';
import { NODE_LOG_LIMIT } from './constants';

function makeConfig(overrides: Partial<ClusterConfig> = {}): ClusterConfig {
  return {
    nodeCount: 3,
    observerIds: [],
    networkConfig: {
      minDelay: 5,
      maxDelay: 10,
      packetLossRate: 0,
      partitions: [],
    },
    electionTimeoutMin: 50,
    electionTimeoutMax: 100,
    heartbeatInterval: 20,
    clientCount: 1,
    ...overrides,
  };
}

describe('SimulationEngine', () => {
  describe('initialization', () => {
    it('creates correct number of nodes', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig({ nodeCount: 5 }), 42);
      expect(engine.getNodes().size).toBe(5);
    });

    it('all nodes start alive', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      for (const [, node] of engine.getNodes()) {
        expect(node.status).toBe('alive');
      }
    });

    it('time starts at 0', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      expect(engine.getTime()).toBe(0);
    });

    it('creates requested number of clients', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig({ clientCount: 3 }), 42);
      expect(engine.getClients().length).toBe(3);
    });
  });

  describe('step', () => {
    it('returns null when no events', () => {
      // Paxos has no initial events scheduled
      const engine = new SimulationEngine(new PaxosAlgorithm(), makeConfig(), 42);
      expect(engine.step()).toBe(null);
    });

    it('processes events in time order', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      const e1 = engine.step();
      const e2 = engine.step();
      if (e1 && e2) {
        expect(e1.time).toBeLessThanOrEqual(e2.time);
      }
    });

    it('advances time to event time', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      engine.step();
      expect(engine.getTime()).toBeGreaterThan(0);
    });
  });

  describe('runUntil', () => {
    it('processes all events up to target time', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      const events = engine.runUntil(200);
      expect(events.length).toBeGreaterThan(0);
      expect(engine.getTime()).toBe(200);
    });

    it('does not process events beyond target time', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      const events = engine.runUntil(10);
      for (const e of events) {
        expect(e.time).toBeLessThanOrEqual(10);
      }
    });
  });

  describe('Raft leader election', () => {
    it('elects a leader after election timeout', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      // Run long enough for election to complete
      engine.runUntil(500);
      const nodes = engine.getNodes();
      const leaders = Array.from(nodes.values()).filter(n => n.role === 'leader');
      expect(leaders.length).toBe(1);
    });

    it('leader has term >= 1', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      engine.runUntil(500);
      const nodes = engine.getNodes();
      const leader = Array.from(nodes.values()).find(n => n.role === 'leader');
      expect(leader).toBeDefined();
      expect(leader!.currentTerm).toBeGreaterThanOrEqual(1);
    });

    it('live stats track leader changes', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      engine.runUntil(500);
      const stats = engine.getLiveStats();
      expect(stats.leaderChanges).toBeGreaterThanOrEqual(1);
      expect(stats.currentLeader).not.toBeNull();
    });
  });

  describe('Raft log replication', () => {
    it('commits a client request', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      // Wait for leader election
      engine.runUntil(500);
      engine.submitClientRequest('cmd_1');
      // Run enough for replication
      engine.runUntil(1000);
      const stats = engine.getLiveStats();
      expect(stats.totalCommits).toBeGreaterThanOrEqual(1);
    });

    it('advances lastApplied when entries commit', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      engine.runUntil(500);
      engine.submitClientRequest('cmd_1');
      engine.runUntil(1000);

      const leader = Array.from(engine.getNodes().values()).find(node => node.role === 'leader');
      expect(leader).toBeDefined();
      expect(leader!.lastApplied).toBeGreaterThanOrEqual(leader!.commitIndex);
      expect(leader!.lastApplied).toBeGreaterThanOrEqual(0);
    });

    it('replicates log entries to followers', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      engine.runUntil(500);
      engine.submitClientRequest('cmd_1');
      engine.runUntil(1000);

      const nodes = engine.getNodes();
      // At least majority should have the entry
      let nodesWithEntry = 0;
      for (const [, node] of nodes) {
        if (node.log.some(e => e.command === 'cmd_1')) nodesWithEntry++;
      }
      expect(nodesWithEntry).toBeGreaterThanOrEqual(2); // majority of 3
    });
  });

  describe('node failure and recovery', () => {
    it('kills a node', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      engine.injectEvent('node_failure', 'node_0', 0);
      engine.step();
      const node = engine.getNodes().get('node_0')!;
      expect(node.status).toBe('dead');
    });

    it('recovers a node', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      engine.injectEvent('node_failure', 'node_0', 0);
      engine.step();
      engine.injectEvent('node_recovery', 'node_0', 1);
      engine.runUntil(2);
      const node = engine.getNodes().get('node_0')!;
      expect(node.status).toBe('alive');
    });

    it('dead nodes ignore messages', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      engine.injectEvent('node_failure', 'node_0', 0);
      engine.step();
      // Try to send a message to dead node
      engine.injectEvent('message_arrive', 'node_0', 1, {
        message: {
          id: 'test', type: 'request_vote', from: 'node_1', to: 'node_0',
          term: 1, payload: { candidateId: 'node_1', lastLogIndex: -1, lastLogTerm: 0 },
        },
      });
      engine.runUntil(2);
      // Node should still be dead, no crash
      expect(engine.getNodes().get('node_0')!.status).toBe('dead');
    });

    it('re-elects leader after leader failure', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig({ nodeCount: 5 }), 42);
      engine.runUntil(500);
      const leader = Array.from(engine.getNodes().values()).find(n => n.role === 'leader');
      expect(leader).toBeDefined();
      // Kill the leader
      engine.injectEvent('node_failure', leader!.id, 501);
      engine.runUntil(1500);
      const nodes = engine.getNodes();
      const newLeaders = Array.from(nodes.values()).filter(
        n => n.role === 'leader' && n.status === 'alive'
      );
      expect(newLeaders.length).toBe(1);
      expect(newLeaders[0].id).not.toBe(leader!.id);
    });
  });

  describe('client management', () => {
    it('addClient increases client count', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig({ clientCount: 1 }), 42);
      expect(engine.getClientCount()).toBe(1);
      engine.addClient();
      expect(engine.getClientCount()).toBe(2);
    });

    it('removeClient decreases client count', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig({ clientCount: 3 }), 42);
      engine.removeClient();
      expect(engine.getClientCount()).toBe(2);
    });

    it('removeClient does not go below 1', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig({ clientCount: 1 }), 42);
      const result = engine.removeClient();
      expect(result).toBeNull();
      expect(engine.getClientCount()).toBe(1);
    });
  });

  describe('cluster status', () => {
    it('has quorum with all nodes alive', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig({ nodeCount: 3 }), 42);
      const { hasQuorum } = engine.getClusterStatus();
      expect(hasQuorum).toBe(true);
    });

    it('loses quorum when majority dies', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig({ nodeCount: 3 }), 42);
      engine.injectEvent('node_failure', 'node_0', 0);
      engine.injectEvent('node_failure', 'node_1', 0);
      engine.runUntil(1);
      const { hasQuorum } = engine.getClusterStatus();
      expect(hasQuorum).toBe(false);
    });

    it('updates network partitions via runtime events', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig({ nodeCount: 3 }), 42);
      engine.injectEvent('network_partition', 'node_0', 0, {
        partitions: [['node_0', 'node_1'], ['node_2']],
      });
      engine.runUntil(1);
      expect(engine.getPartitions()).toEqual([['node_0', 'node_1'], ['node_2']]);

      engine.injectEvent('heal_partition', 'node_0', 2);
      engine.runUntil(3);
      expect(engine.getPartitions()).toEqual([]);
    });
  });

  describe('metrics', () => {
    it('records commit latencies', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      engine.runUntil(500);
      engine.submitClientRequest('test_cmd');
      engine.runUntil(1000);
      const metrics = engine.getMetrics();
      if (engine.getLiveStats().totalCommits > 0) {
        expect(metrics.commitLatencies.length).toBeGreaterThan(0);
        expect(metrics.commitTimestamps.length).toBe(metrics.commitLatencies.length);
      }
    });

    it('tracks leader change timestamps', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      engine.runUntil(500);
      const metrics = engine.getMetrics();
      expect(metrics.leaderChangeTimestamps.length).toBeGreaterThanOrEqual(1);
    });

    it('replays the same history for the same seed', () => {
      const a = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      const b = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);

      a.runUntil(500);
      b.runUntil(500);
      a.submitClientRequest('cmd_1');
      b.submitClientRequest('cmd_1');
      a.runUntil(1200);
      b.runUntil(1200);

      const snapshot = (engine: SimulationEngine) => ({
        stats: engine.getLiveStats(),
        metrics: engine.getMetrics(),
        nodes: Array.from(engine.getNodes().values()).map(node => ({
          id: node.id,
          role: node.role,
          term: node.currentTerm,
          logBaseIndex: node.logBaseIndex,
          logBaseTerm: node.logBaseTerm,
          commitIndex: node.commitIndex,
          log: node.log.map(entry => ({
            entryId: entry.entryId,
            requestId: entry.requestId,
            index: entry.index,
            term: entry.term,
            command: entry.command,
            committed: entry.committed,
          })),
        })),
      });

      expect(snapshot(a)).toEqual(snapshot(b));
    });
  });

  describe('history trimming', () => {
    it('keeps global log indices stable after trimming', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      const node = engine.getNodes().get('node_0')!;

      node.log = Array.from({ length: NODE_LOG_LIMIT + 5 }, (_, i) => ({
        entryId: `entry_${i}`,
        requestId: `req_${i}`,
        term: 1,
        index: i,
        command: `cmd_${i}`,
        committed: true,
      }));
      node.commitIndex = NODE_LOG_LIMIT + 4;
      node.lastApplied = NODE_LOG_LIMIT + 4;

      (engine as unknown as { trimHistory: () => void }).trimHistory();

      expect(node.log.length).toBe(NODE_LOG_LIMIT);
      expect(node.logBaseIndex).toBe(5);
      expect(node.logBaseTerm).toBe(1);
      expect(node.commitIndex).toBe(NODE_LOG_LIMIT + 4);
      expect(node.lastApplied).toBe(NODE_LOG_LIMIT + 4);
      expect(node.log[0].index).toBe(5);
    });
  });

  describe('timeout progress', () => {
    it('returns progress for active timeouts', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);
      // At time 0, all nodes have election timeouts
      const progress = engine.getTimeoutProgress();
      expect(progress.size).toBeGreaterThan(0);
    });
  });

  describe('integration scenarios', () => {
    it('Raft recovers a follower and catches it up after missed commits', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig(), 42);

      engine.runUntil(500);
      engine.injectEvent('node_failure', 'node_2', 501);
      engine.runUntil(520);

      engine.submitClientRequest('cmd_a');
      engine.runUntil(1100);

      engine.injectEvent('node_recovery', 'node_2', 1101);
      engine.runUntil(1250);

      engine.submitClientRequest('cmd_b');
      engine.runUntil(2200);

      const recovered = engine.getNodes().get('node_2')!;
      expect(recovered.status).toBe('alive');
      expect(recovered.log.filter(entry => entry.committed).map(entry => entry.command)).toEqual(['cmd_a', 'cmd_b']);
      expect(recovered.commitIndex).toBeGreaterThanOrEqual(1);
      expect(recovered.lastApplied).toBeGreaterThanOrEqual(recovered.commitIndex);
    });

    it('Raft resumes progress after partition heal', () => {
      const engine = new SimulationEngine(new RaftAlgorithm(), makeConfig({ nodeCount: 5 }), 42);

      engine.runUntil(500);
      engine.injectEvent('network_partition', 'node_0', 501, {
        partitions: [['node_0', 'node_1', 'node_2'], ['node_3', 'node_4']],
      });
      engine.runUntil(520);

      engine.submitClientRequest('during_partition');
      engine.runUntil(1200);

      const minorityBeforeHeal = ['node_3', 'node_4'].map(id => engine.getNodes().get(id)!);
      expect(minorityBeforeHeal.every(node => !node.log.some(entry => entry.command === 'during_partition' && entry.committed))).toBe(true);

      engine.injectEvent('heal_partition', 'node_0', 1201);
      engine.runUntil(1500);
      const leaderAfterHeal = Array.from(engine.getNodes().values()).find(node => node.role === 'leader' && node.status === 'alive');
      expect(leaderAfterHeal).toBeDefined();
      engine.submitClientRequest('after_heal', 'client_0', leaderAfterHeal!.id);
      engine.runUntil(2800);

      expect(engine.getPartitions()).toEqual([]);
      expect(engine.getLiveStats().currentLeader).not.toBeNull();
      expect(engine.getLiveStats().totalCommits).toBeGreaterThanOrEqual(1);
      const afterHealReplicas = Array.from(engine.getNodes().values()).filter(node =>
        node.status === 'alive' && node.log.some(entry => entry.command === 'after_heal' && entry.committed));
      expect(afterHealReplicas.length).toBeGreaterThanOrEqual(3);
    });

    it('Multi-Paxos commits repeated identical commands as distinct slots', () => {
      const engine = new SimulationEngine(new MultiPaxosAlgorithm(), makeConfig(), 42);
      const nodes = engine.getNodes();
      const leader = nodes.get('node_0')!;
      const followerA = nodes.get('node_1')!;
      const followerB = nodes.get('node_2')!;
      const internals = engine as unknown as { cancelTimeout: (nodeId: string, type: 'election' | 'heartbeat') => void };

      leader.role = 'leader';
      leader.currentTerm = 7;
      leader.meta.knownLeader = 'node_0';
      leader.meta.leaderBallot = 7;
      leader.meta.leaderEstablished = true;

      for (const follower of [followerA, followerB]) {
        follower.role = 'follower';
        follower.currentTerm = 7;
        follower.meta.knownLeader = 'node_0';
        follower.meta.minProposal = 7;
      }

      for (const nodeId of ['node_0', 'node_1', 'node_2']) {
        internals.cancelTimeout(nodeId, 'election');
      }

      engine.injectEvent('client_request', 'node_0', 1, {
        command: 'set x=1',
        clientId: 'client_0',
        requestId: 'req_mp_1',
        entryId: 'entry_mp_1',
      });
      engine.injectEvent('client_request', 'node_0', 40, {
        command: 'set x=1',
        clientId: 'client_0',
        requestId: 'req_mp_2',
        entryId: 'entry_mp_2',
      });
      engine.runUntil(250);

      expect(engine.getLiveStats().totalCommits).toBeGreaterThanOrEqual(2);

      const nodesWithRepeatedCommit = Array.from(nodes.values()).filter(node => {
        const committed = node.log.filter(entry => entry.committed && entry.command === 'set x=1');
        return committed.length === 2
          && committed[0].index === 0
          && committed[1].index === 1
          && new Set(committed.map(entry => entry.entryId)).size === 2;
      });

      expect(nodesWithRepeatedCommit.length).toBeGreaterThanOrEqual(1);
    });

    it('Zab synchronizes a recovered follower with committed zxid entries', () => {
      const engine = new SimulationEngine(new ZabAlgorithm(), makeConfig(), 42);
      const nodes = engine.getNodes();
      const leader = nodes.get('node_0')!;
      const followerA = nodes.get('node_1')!;
      const followerB = nodes.get('node_2')!;
      const internals = engine as unknown as { cancelTimeout: (nodeId: string, type: 'election' | 'heartbeat') => void };

      leader.role = 'leading';
      leader.currentTerm = 1;
      leader.meta.epoch = 1;
      leader.meta.phase = 'broadcast';
      leader.meta.knownLeader = 'node_0';
      leader.meta.counter = 0;

      for (const follower of [followerA, followerB]) {
        follower.role = 'following';
        follower.currentTerm = 1;
        follower.meta.epoch = 1;
        follower.meta.phase = 'broadcast';
        follower.meta.knownLeader = 'node_0';
      }

      for (const nodeId of ['node_0', 'node_1', 'node_2']) {
        internals.cancelTimeout(nodeId, 'election');
      }

      engine.injectEvent('node_failure', 'node_2', 1);
      engine.injectEvent('client_request', 'node_0', 10, {
        command: 'zab_cmd',
        clientId: 'client_0',
        requestId: 'req_zab_1',
        entryId: 'entry_zab_1',
      });
      engine.runUntil(160);

      engine.injectEvent('node_recovery', 'node_2', 161);
      engine.runUntil(500);

      const recovered = engine.getNodes().get('node_2')!;
      const committed = recovered.log.filter(entry => entry.committed);
      expect(committed.some(entry => entry.command === 'zab_cmd')).toBe(true);
      expect(committed.every(entry => entry.zxid !== undefined)).toBe(true);
    });

    it('EPaxos commits requests from different replicas with instance identities preserved', () => {
      const engine = new SimulationEngine(new EPaxosAlgorithm(), makeConfig({ clientCount: 2 }), 42);

      engine.submitClientRequest('cmd_left', 'client_0', 'node_0');
      engine.submitClientRequest('cmd_right', 'client_1', 'node_1');
      engine.runUntil(1200);

      expect(engine.getLiveStats().totalCommits).toBeGreaterThanOrEqual(2);

      for (const node of engine.getNodes().values()) {
        const committed = node.log.filter(entry => entry.committed);
        expect(committed.map(entry => entry.command)).toEqual(['cmd_left', 'cmd_right']);
        expect(committed.every(entry => entry.instanceKey !== undefined)).toBe(true);
      }
    });
  });
});
