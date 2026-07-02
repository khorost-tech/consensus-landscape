export type NodeId = string;

export type NodeRole = 'follower' | 'candidate' | 'leader' | 'proposer' | 'acceptor' | 'learner'
  | 'looking' | 'leading' | 'following' | 'replica';

export type NodeStatus = 'alive' | 'dead';

export interface ClientRequest {
  requestId: string;
  entryId: string;
  command: string;
}

export interface LogEntry {
  entryId: string;
  requestId: string;
  zxid?: string;
  instanceKey?: string;
  term: number;
  index: number;
  command: string;
  committed: boolean;
}

export interface NodeState {
  id: NodeId;
  role: NodeRole;
  status: NodeStatus;
  currentTerm: number;
  votedFor: NodeId | null;
  log: LogEntry[];
  /** Global index of the first in-memory log entry (after trimming/compaction). */
  logBaseIndex: number;
  /** Term of the last compacted entry at index logBaseIndex - 1. */
  logBaseTerm: number;
  commitIndex: number;
  lastApplied: number;
  // Raft leader state
  nextIndex: Map<NodeId, number>;
  matchIndex: Map<NodeId, number>;
  // Raft election
  votesReceived: Set<NodeId>;
  // Generic metadata for algorithm-specific state
  meta: Record<string, unknown>;
}

export type MessageType =
  // Raft messages
  | 'request_vote'
  | 'request_vote_response'
  | 'append_entries'
  | 'append_entries_response'
  | 'install_snapshot'
  | 'install_snapshot_response'
  // Paxos messages
  | 'prepare'
  | 'promise'
  | 'accept'
  | 'accepted'
  | 'nack'
  | 'learn'
  // Multi-Paxos
  | 'mp_heartbeat'
  | 'mp_heartbeat_response'
  // Zab messages
  | 'zab_election'
  | 'zab_election_ack'
  | 'zab_followerinfo'
  | 'zab_newleader'
  | 'zab_ack_newleader'
  | 'zab_proposal'
  | 'zab_ack'
  | 'zab_commit'
  | 'zab_sync'
  // EPaxos messages
  | 'ep_preaccept'
  | 'ep_preaccept_ok'
  | 'ep_accept'
  | 'ep_accept_ok'
  | 'ep_commit'
  // Client
  | 'client_request'
  | 'client_response';

export interface Message {
  id: string;
  type: MessageType;
  from: NodeId;
  to: NodeId;
  term: number;
  payload: Record<string, unknown>;
}

export type TimeoutType = 'election' | 'heartbeat' | 'proposal';

export type EventType =
  | 'message_arrive'
  | 'timeout'
  | 'client_request'
  | 'node_failure'
  | 'node_recovery'
  | 'message_send'
  | 'network_partition'
  | 'heal_partition';

export interface SimEvent {
  id: string;
  time: number;
  type: EventType;
  target: NodeId;
  payload: {
    message?: Message;
    timeoutType?: TimeoutType;
    command?: string;
    requestId?: string;
    entryId?: string;
    clientId?: string; // which client sent this request
    targetNode?: NodeId;
    partitions?: NodeId[][];
  };
}

export interface Action {
  type: 'send_message' | 'set_timeout' | 'cancel_timeout' | 'commit_entry' | 'apply_entry';
  message?: Omit<Message, 'id'>;
  timeout?: { type: TimeoutType; duration: number; nodeId: NodeId };
}

export interface NetworkConfig {
  minDelay: number;      // ms in virtual time
  maxDelay: number;
  packetLossRate: number; // 0..1
  partitions: NodeId[][]; // groups of nodes that can't communicate across groups
}

export interface ClusterConfig {
  nodeCount: number;
  observerIds: NodeId[];  // nodes that don't vote
  networkConfig: NetworkConfig;
  electionTimeoutMin: number;
  electionTimeoutMax: number;
  heartbeatInterval: number;
  /** Initial number of clients (default 2) */
  clientCount: number;
}

export interface SimulationSnapshot {
  time: number;
  nodes: Map<NodeId, NodeState>;
  pendingMessages: Message[];
  metrics: SimulationMetrics;
}

export interface SimulationMetrics {
  /** Parallel arrays: commitTimestamps[i] is when commitLatencies[i] was recorded */
  commitTimestamps: number[];
  commitLatencies: number[];
  /** Timestamps when leader changes happened */
  leaderChangeTimestamps: number[];
  /** Node failure/recovery events for graph annotations */
  nodeEvents: Array<{ time: number; type: 'failure' | 'recovery'; nodeId: NodeId }>;
  /** Timestamps of Paxos NACK conflicts */
  conflictTimestamps: number[];
  /** Cluster status zones: periods without quorum or during elections */
  statusZones: Array<{ start: number; end: number; type: 'no_quorum' | 'electing' }>;
}

export interface ScenarioMarker {
  time: number;
  type: ScenarioEvent['type'];
  label: string;
  detail?: string;
}

/** Live counters updated every step — for panel header display */
export interface LiveStats {
  totalMessages: number;
  droppedMessages: number;
  totalCommits: number;
  leaderChanges: number;
  avgLatency: number;      // rolling average of last N commit latencies
  currentLeader: NodeId | null;
  currentTerm: number;
  /** Time (virtual ms) from start until first leader elected (Raft) / first commit (Paxos) */
  electionTime: number | null;
  /** How many client requests were rejected/redirected because cluster wasn't ready */
  rejectedRequests: number;
  // Paxos specific
  nackCount: number;        // proposal rejections (competing proposers)
  quorumSize: number;       // majority needed
}

export interface ClientState {
  id: string;
  pendingCommand: string | null;
  targetNode: NodeId | null;
  completedCommands: number;
  lastLatency: number | null;
}

export interface ScenarioEvent {
  time: number;
  type: 'kill_node' | 'recover_node' | 'partition' | 'heal_partition' | 'client_write';
  params: Record<string, unknown>;
}

export interface Scenario {
  id: string;
  name: string;
  description: string;
  clusterConfig: ClusterConfig;
  events: ScenarioEvent[];
}
