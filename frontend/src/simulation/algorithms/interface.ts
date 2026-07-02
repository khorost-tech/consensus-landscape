import { NodeState, Message, Action, ClusterConfig, TimeoutType, NodeId, ClientRequest } from '../types';

export interface ConsensusAlgorithm {
  readonly name: string;
  readonly description: string;

  /** Inject deterministic random source from the engine. */
  setRandomSource(rng: () => number): void;

  /** Create initial state for a node */
  getInitialState(nodeId: string, config: ClusterConfig): NodeState;

  /** Handle incoming message */
  onMessage(node: NodeState, msg: Message): Action[];

  /** Handle timeout (election or heartbeat) */
  onTimeout(node: NodeState, type: TimeoutType): Action[];

  /** Handle client request (write command).
   *  Return actions — may include a redirect response if node is not leader. */
  onClientRequest(node: NodeState, request: ClientRequest): Action[];

  /** Handle node recovery after failure */
  onRecovery(node: NodeState, config: ClusterConfig): Action[];

  /** Can this node accept client requests right now? */
  canAcceptClientRequest(node: NodeState): boolean;

  /** Get the node this node believes is the leader (for redirect) */
  getKnownLeader(node: NodeState): NodeId | null;
}
