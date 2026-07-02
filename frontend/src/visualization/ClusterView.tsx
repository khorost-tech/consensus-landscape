import React, { useMemo, useState, useCallback } from 'react';
import { NodeId, NodeState, ClientState, ClientRequest } from '../simulation/types';
import { ActiveMessage, ClientConnection } from '../simulation/engine';
import { TimeoutProgressMap } from '../hooks/useSimulation';
import { AUTO_CLIENT_INTERVAL } from '../simulation/constants';

interface ClusterViewProps {
  nodes: Map<NodeId, NodeState>;
  activeMessages: ActiveMessage[];
  clients: ClientState[];
  currentTime: number;
  onNodeClick?: (nodeId: NodeId) => void;
  selectedNode?: NodeId | null;
  timeoutProgress?: TimeoutProgressMap;
  clientConnections?: ClientConnection[];
  autoClientTiming?: Map<string, number>;
  width?: number;
  height?: number;
}

const ROLE_COLORS: Record<string, { fill: string; stroke: string; label: string }> = {
  follower:  { fill: 'var(--node-follower)',  stroke: 'var(--node-follower-stroke)',  label: 'F' },
  candidate: { fill: 'var(--node-candidate)', stroke: 'var(--node-candidate-stroke)', label: 'C' },
  leader:    { fill: 'var(--node-leader)',    stroke: 'var(--node-leader-stroke)',    label: 'L' },
  proposer:  { fill: 'var(--node-candidate)', stroke: 'var(--node-candidate-stroke)', label: 'P' },
  acceptor:  { fill: 'var(--node-follower)',  stroke: 'var(--node-follower-stroke)',  label: 'A' },
  learner:   { fill: 'var(--node-follower)',  stroke: 'var(--node-follower-stroke)',  label: 'Ln' },
  // Zab roles
  looking:   { fill: 'var(--node-candidate)', stroke: 'var(--node-candidate-stroke)', label: 'E' },
  leading:   { fill: 'var(--node-leader)',    stroke: 'var(--node-leader-stroke)',    label: 'ZL' },
  following: { fill: 'var(--node-follower)',  stroke: 'var(--node-follower-stroke)',  label: 'ZF' },
  // EPaxos role
  replica:   { fill: 'var(--node-follower)',  stroke: 'var(--node-follower-stroke)',  label: 'R' },
};

/** Message visual config: shape, color, size */
type MsgShape = 'circle' | 'diamond' | 'square' | 'triangle';
interface MsgVisual { color: string; shape: MsgShape; size: number; label: string }

const MSG_VISUALS: Record<string, MsgVisual> = {
  // Client messages — yellow square (envelope)
  client_request:   { color: 'var(--msg-client)', shape: 'square',   size: 7, label: '' },
  client_response:  { color: 'var(--msg-replication)', shape: 'square', size: 6, label: '✓' },
  // Raft election — purple diamond
  request_vote:          { color: 'var(--msg-vote)',   shape: 'diamond',  size: 5, label: 'V' },
  request_vote_response: { color: 'var(--msg-vote)',   shape: 'diamond',  size: 4, label: '' },
  // Raft heartbeat (empty append_entries) — green circle (small)
  append_entries_heartbeat:          { color: 'var(--msg-replication)', shape: 'circle', size: 3, label: '' },
  append_entries_heartbeat_response: { color: 'var(--msg-replication)', shape: 'circle', size: 2, label: '' },
  // Raft replication with data — green square (envelope)
  append_entries:          { color: 'var(--msg-replication)', shape: 'square', size: 6, label: '' },
  append_entries_response: { color: 'var(--msg-replication)', shape: 'square', size: 4, label: '' },
  install_snapshot:          { color: 'var(--msg-replication)', shape: 'square', size: 7, label: 'S' },
  install_snapshot_response: { color: 'var(--msg-replication)', shape: 'circle', size: 3, label: '' },
  // Paxos phase 1 — purple diamond
  prepare: { color: 'var(--msg-vote)',   shape: 'diamond',  size: 5, label: 'P' },
  promise: { color: 'var(--msg-vote)',   shape: 'diamond',  size: 4, label: '' },
  // Paxos phase 2 — green square with 'A' label
  accept:   { color: 'var(--msg-replication)', shape: 'square', size: 6, label: 'A' },
  accepted: { color: 'var(--msg-replication)', shape: 'square', size: 4, label: '' },
  // Nack — red
  nack: { color: 'var(--msg-nack)', shape: 'diamond', size: 4, label: '✗' },
  // Paxos learn — green circle (committed notification)
  learn: { color: 'var(--msg-replication)', shape: 'circle', size: 4, label: 'L' },
  // Multi-Paxos heartbeat
  mp_heartbeat:          { color: 'var(--msg-replication)', shape: 'circle',  size: 3, label: '' },
  mp_heartbeat_response: { color: 'var(--msg-replication)', shape: 'circle',  size: 2, label: '' },
  // Zab election
  zab_election:      { color: 'var(--msg-vote)',        shape: 'diamond',  size: 5, label: 'E' },
  zab_election_ack:  { color: 'var(--msg-vote)',        shape: 'diamond',  size: 4, label: '' },
  zab_followerinfo:  { color: 'var(--msg-vote)',        shape: 'diamond',  size: 4, label: 'I' },
  zab_newleader:     { color: 'var(--msg-vote)',        shape: 'diamond',  size: 5, label: 'N' },
  zab_ack_newleader: { color: 'var(--msg-vote)',        shape: 'diamond',  size: 4, label: '' },
  zab_sync:          { color: 'var(--msg-replication)', shape: 'square',   size: 5, label: 'S' },
  zab_proposal:      { color: 'var(--msg-replication)', shape: 'square',   size: 6, label: 'P' },
  zab_ack:           { color: 'var(--msg-replication)', shape: 'square',   size: 4, label: '' },
  zab_commit:        { color: 'var(--msg-replication)', shape: 'circle',   size: 4, label: 'C' },
  // EPaxos fast path
  ep_preaccept:    { color: 'var(--msg-replication)', shape: 'triangle', size: 5, label: 'PA' },
  ep_preaccept_ok: { color: 'var(--msg-replication)', shape: 'triangle', size: 4, label: '' },
  // EPaxos slow path
  ep_accept:       { color: 'var(--msg-vote)',        shape: 'diamond',  size: 6, label: 'A' },
  ep_accept_ok:    { color: 'var(--msg-vote)',        shape: 'diamond',  size: 4, label: '' },
  // EPaxos commit
  ep_commit:       { color: 'var(--msg-replication)', shape: 'circle',   size: 4, label: 'C' },
};

/** Determine visual key for a message — distinguishes heartbeat from data replication */
function getMsgVisualKey(msg: { type: string; payload: Record<string, unknown> }): string {
  if (msg.type === 'append_entries') {
    const entries = msg.payload.entries as unknown[] | undefined;
    return (!entries || entries.length === 0) ? 'append_entries_heartbeat' : 'append_entries';
  }
  if (msg.type === 'append_entries_response') {
    // Response to heartbeat has no matchIndex change, but we can check if original had entries
    // Simpler: check if this is ack for data — use 'isHeartbeatResponse' flag if available
    const isHb = msg.payload.isHeartbeat;
    return isHb ? 'append_entries_heartbeat_response' : 'append_entries_response';
  }
  return msg.type;
}

function getNodePosition(index: number, total: number, cx: number, cy: number, radius: number) {
  const angle = (2 * Math.PI * index) / total - Math.PI / 2;
  return {
    x: cx + radius * Math.cos(angle),
    y: cy + radius * Math.sin(angle),
  };
}

/** Render message shape at (0,0) */
function MsgShapeEl({ visual }: { visual: MsgVisual }) {
  const s = visual.size;
  switch (visual.shape) {
    case 'diamond':
      return <polygon points={`0,${-s} ${s},0 0,${s} ${-s},0`} fill={visual.color} opacity={0.9} />;
    case 'square':
      return <rect x={-s} y={-s} width={s * 2} height={s * 2} fill={visual.color} opacity={0.9} rx={1} />;
    case 'triangle':
      return <polygon points={`0,${-s} ${s},${s * 0.7} ${-s},${s * 0.7}`} fill={visual.color} opacity={0.9} />;
    default:
      return <circle r={s} fill={visual.color} opacity={0.9} />;
  }
}

const ZOOM_LEVELS = [0.5, 0.625, 0.75, 0.875, 1, 1.125, 1.25, 1.5, 1.75, 2];

/** Circular progress arc starting from 12 o'clock */
const TimeoutArc = React.memo(function TimeoutArc({ cx, cy, r, progress, color, width: strokeW }: {
  cx: number; cy: number; r: number; progress: number; color: string; width?: number;
}) {
  if (progress <= 0) return null;
  const circumference = 2 * Math.PI * r;
  const arcLen = circumference * Math.min(progress, 1);
  // SVG circle starts at 3 o'clock; offset by -25% circumference to start at 12 o'clock
  return (
    <circle
      cx={cx} cy={cy} r={r}
      fill="none"
      stroke={color}
      strokeWidth={strokeW ?? 2.5}
      strokeDasharray={`${arcLen} ${circumference - arcLen}`}
      strokeDashoffset={circumference * 0.25}
      strokeLinecap="round"
      opacity={0.7}
    />
  );
});

export const ClusterView: React.FC<ClusterViewProps> = React.memo(({
  nodes, activeMessages, clients, currentTime, onNodeClick,
  selectedNode, timeoutProgress, clientConnections, autoClientTiming,
  width = 500, height = 400,
}) => {
  const [zoom, setZoom] = useState(1);
  const handleZoomIn = useCallback(() => {
    setZoom(z => { const i = ZOOM_LEVELS.indexOf(z); return i < ZOOM_LEVELS.length - 1 ? ZOOM_LEVELS[i + 1] : z; });
  }, []);
  const handleZoomOut = useCallback(() => {
    setZoom(z => { const i = ZOOM_LEVELS.indexOf(z); return i > 0 ? ZOOM_LEVELS[i - 1] : z; });
  }, []);

  const vw = width / zoom;
  const vh = height / zoom;
  const cx = width / 2;
  const cy = height / 2;
  const clusterRadius = Math.min(width, height) * 0.28;
  const nodeRadius = Math.min(24, clusterRadius * 0.22);

  const nodeEntries = useMemo(() => Array.from(nodes.entries()), [nodes]);

  const nodePositions = useMemo(() => {
    const positions = new Map<NodeId, { x: number; y: number }>();
    nodeEntries.forEach(([id], i) => {
      positions.set(id, getNodePosition(i, nodeEntries.length, cx, cy, clusterRadius));
    });
    return positions;
  }, [nodeEntries, cx, cy, clusterRadius]);

  const clientPositions = useMemo(() => {
    const outerRadius = clusterRadius + 50;
    const count = clients.length;
    return clients.map((client, i) => {
      const spread = Math.min(1.2, 0.4 * count);
      const step = count > 1 ? spread / (count - 1) : 0;
      const angle = -Math.PI / 2 - spread / 2 + i * step;
      return {
        client,
        x: cx + outerRadius * Math.cos(angle),
        y: cy + outerRadius * Math.sin(angle),
      };
    });
  }, [clients, cx, cy, clusterRadius]);

  // Build lookup for client positions by id
  const clientPosMap = useMemo(() => {
    const m = new Map<string, { x: number; y: number }>();
    clientPositions.forEach(cp => m.set(cp.client.id, { x: cp.x, y: cp.y }));
    return m;
  }, [clientPositions]);

  const fontSize = 11;

  // Connection state colors
  const CONNECTION_COLORS: Record<string, string> = {
    pending: 'var(--msg-client)',       // yellow while request is in-flight
    replicating: 'var(--msg-client)',   // yellow while replicating
    committed: 'var(--msg-replication)', // green when committed
  };

  return (
    <div className="cluster-view-wrapper">
      <svg
        viewBox={`${(width - vw) / 2} ${(height - vh) / 2} ${vw} ${vh}`}
        width="100%"
        height="100%"
        preserveAspectRatio="xMidYMid meet"
      >
        {/* Node-to-node connection lines */}
        {nodeEntries.map(([id], i) => {
          const from = nodePositions.get(id)!;
          return nodeEntries.slice(i + 1).map(([otherId]) => {
            const to = nodePositions.get(otherId)!;
            return (
              <line key={`line-${id}-${otherId}`}
                x1={from.x} y1={from.y} x2={to.x} y2={to.y}
                stroke="var(--line-color)" strokeWidth={0.5} opacity={0.12}
              />
            );
          });
        })}

        {/* Client connection lines — color reflects state */}
        {(clientConnections ?? []).map(conn => {
          const cPos = clientPosMap.get(conn.clientId);
          const nPos = nodePositions.get(conn.targetNode);
          if (!cPos || !nPos) return null;
          // Don't draw connection to dead nodes
          const connTargetState = nodes.get(conn.targetNode);
          if (connTargetState?.status === 'dead') return null;
          const color = CONNECTION_COLORS[conn.state] ?? 'var(--msg-client)';
          const isCommitted = conn.state === 'committed';
          return (
            <line key={`conn-${conn.command}`}
              x1={cPos.x} y1={cPos.y} x2={nPos.x} y2={nPos.y}
              stroke={color}
              strokeWidth={isCommitted ? 2.5 : 1.8}
              strokeDasharray={isCommitted ? 'none' : '5 3'}
              opacity={isCommitted ? 0.8 : 0.5}
            />
          );
        })}

        {/* Fallback: client-to-target dashed lines for clients without connection state */}
        {clientPositions.map(({ client, x, y }) => {
          if (!client.targetNode) return null;
          // Skip if already drawn by clientConnections
          if (clientConnections?.some(c => c.clientId === client.id)) return null;
          // Don't draw line to dead nodes
          const targetState = nodes.get(client.targetNode);
          if (targetState?.status === 'dead') return null;
          const target = nodePositions.get(client.targetNode);
          if (!target) return null;
          return (
            <line key={`cl-${client.id}`}
              x1={x} y1={y} x2={target.x} y2={target.y}
              stroke="var(--msg-client)" strokeWidth={1.5} strokeDasharray="4 3" opacity={0.3}
            />
          );
        })}

        {/* Active messages */}
        {activeMessages.filter(am => !am.dropped).map(am => {
          const cFrom = clientPosMap.get(am.message.from);
          const cTo = clientPosMap.get(am.message.to);
          const from = nodePositions.get(am.message.from) ?? cFrom;
          const to = nodePositions.get(am.message.to) ?? cTo;
          if (!from || !to) return null;

          const deliverTime = am.deliverTime ?? am.arriveTime;
          const totalTime = deliverTime - am.sendTime;
          if (totalTime <= 0) return null;
          const progress = Math.max(0, Math.min(1, (currentTime - am.sendTime) / totalTime));

          // Client messages fly straight; inter-node messages curve
          const isClientMsg = !!cFrom || !!cTo;
          const curvature = isClientMsg ? 0 : 0.2;

          const midX = (from.x + to.x) / 2;
          const midY = (from.y + to.y) / 2;
          const dx = to.x - from.x;
          const dy = to.y - from.y;
          const ctrlX = midX - dy * curvature;
          const ctrlY = midY + dx * curvature;

          const t = progress;
          const mt = 1 - t;
          const x = mt * mt * from.x + 2 * mt * t * ctrlX + t * t * to.x;
          const y = mt * mt * from.y + 2 * mt * t * ctrlY + t * t * to.y;

          const visualKey = getMsgVisualKey(am.message);
          const visual = MSG_VISUALS[visualKey] ?? { color: 'var(--text-secondary)', shape: 'circle' as MsgShape, size: 3, label: '' };

          return (
            <g key={`msg-${am.message.id}`} transform={`translate(${x},${y})`}>
              <MsgShapeEl visual={visual} />
              {visual.label && (
                <text textAnchor="middle" dominantBaseline="central"
                  fontSize={visual.size * 1.2} fontWeight="bold" fill="white">
                  {visual.label}
                </text>
              )}
              <title>{am.message.type}: {am.message.from} → {am.message.to} (T{am.message.term})</title>
            </g>
          );
        })}

        {/* Nodes */}
        {nodeEntries.map(([id, node]) => {
          const pos = nodePositions.get(id)!;
          const isDead = node.status === 'dead';
          const roleInfo = ROLE_COLORS[node.role] ?? ROLE_COLORS.follower;
          const nodeTimeouts = timeoutProgress?.get(id) ?? [];
          const isSelected = selectedNode === id;

          // Separate heartbeat and election timeouts
          const heartbeat = nodeTimeouts.find(t => t.type === 'heartbeat');
          const election = nodeTimeouts.find(t => t.type === 'election');

          return (
            <g key={id} onClick={() => onNodeClick?.(id)} style={{ cursor: 'pointer' }}>
              {/* Selection highlight ring */}
              {isSelected && (
                <circle
                  cx={pos.x} cy={pos.y} r={nodeRadius + 6}
                  fill="none"
                  stroke="var(--node-selected, #ff9800)"
                  strokeWidth={2.5}
                  opacity={0.9}
                  className="selected-ring"
                />
              )}
              <circle
                cx={pos.x} cy={pos.y} r={nodeRadius}
                fill={isDead ? 'var(--node-dead)' : roleInfo.fill}
                stroke={isDead ? 'var(--node-dead-stroke)' : roleInfo.stroke}
                strokeWidth={node.role === 'leader' || node.role === 'leading' ? 3 : 2}
                opacity={isDead ? 0.4 : 1}
              />
              {/* Heartbeat arc (leader only) — big green */}
              {!isDead && heartbeat && (
                <TimeoutArc
                  cx={pos.x} cy={pos.y}
                  r={nodeRadius + 4}
                  progress={heartbeat.progress}
                  color="var(--node-leader)"
                  width={3.5}
                />
              )}
              {/* Election timeout arc (followers/candidates) — purple, thinner */}
              {!isDead && election && (
                <TimeoutArc
                  cx={pos.x} cy={pos.y}
                  r={nodeRadius + 4}
                  progress={election.progress}
                  color="var(--msg-vote)"
                  width={2}
                />
              )}
              {/* Vote badge for Raft candidates */}
              {!isDead && node.role === 'candidate' && (() => {
                const totalPeers = (node.meta.peers as string[] | undefined)?.length ?? 0;
                const majority = Math.floor((totalPeers + 1) / 2) + 1;
                const collected = node.votesReceived.size;
                const ratio = collected / majority;
                return (
                  <g>
                    <TimeoutArc cx={pos.x} cy={pos.y} r={nodeRadius + 4}
                      progress={Math.min(ratio, 1)} color="var(--msg-vote)" width={2} />
                    <circle cx={pos.x - nodeRadius * 0.7} cy={pos.y - nodeRadius * 0.7} r={7}
                      fill="var(--msg-vote)" opacity={0.9} />
                    <text x={pos.x - nodeRadius * 0.7} y={pos.y - nodeRadius * 0.7}
                      textAnchor="middle" dominantBaseline="central"
                      fontSize={7} fontWeight="bold" fill="white">
                      {collected}/{majority}
                    </text>
                  </g>
                );
              })()}
              {/* Paxos proposal status: command + phase + count + queue */}
              {!isDead && node.role === 'proposer' && (() => {
                const totalPeers = (node.meta.peers as string[] | undefined)?.length ?? 0;
                const majority = Math.floor((totalPeers + 1) / 2) + 1;
                const promises = (node.meta.promisesReceived as number) ?? 0;
                const accepts = (node.meta.acceptsReceived as number) ?? 0;
                const phase = node.meta.proposalPhase as 'prepare' | 'accept' | null;
                const pendingVal = node.meta.pendingValue as ClientRequest | null;
                const queue = (node.meta.commandQueue as ClientRequest[]) ?? [];
                const inPrepare = phase === 'prepare';
                const collected = inPrepare ? promises : accepts;
                const phaseLabel = inPrepare ? 'P' : 'A';
                const cmdLabel = pendingVal ? pendingVal.command.replace('cmd_', '#') : '?';
                // Arc progress excludes self-count so it starts from 0
                const externalNeeded = majority - 1; // how many external responses needed
                const externalCollected = Math.max(0, collected - 1); // minus self
                const ratio = externalNeeded > 0 ? externalCollected / externalNeeded : 1;
                const badgeY = pos.y + nodeRadius + 20;
                const queued = queue.filter(request => request.requestId !== pendingVal?.requestId);
                const lineH = 13;
                const badgeW = 62;
                return (
                  <g>
                    <TimeoutArc cx={pos.x} cy={pos.y} r={nodeRadius + 4}
                      progress={Math.min(ratio, 1)} color="var(--msg-vote)" width={2} />
                    {/* Active proposal */}
                    <rect x={pos.x - badgeW / 2} y={badgeY - lineH / 2}
                      width={badgeW} height={lineH} rx={3}
                      fill="var(--msg-vote)" opacity={0.85} />
                    <text x={pos.x} y={badgeY}
                      textAnchor="middle" dominantBaseline="central"
                      fontSize={8} fontWeight="bold" fill="white">
                      {cmdLabel} {phaseLabel} {collected}/{majority}
                    </text>
                    {/* Queued commands */}
                    {queued.slice(0, 3).map((request, qi) => {
                      const qy = badgeY + lineH * (qi + 1);
                      return (
                        <g key={request.requestId}>
                          <rect x={pos.x - badgeW / 2} y={qy - lineH / 2}
                            width={badgeW} height={lineH} rx={3}
                            fill="var(--bg-secondary, #555)" opacity={0.6} />
                          <text x={pos.x} y={qy}
                            textAnchor="middle" dominantBaseline="central"
                            fontSize={7} fill="var(--text-secondary)">
                            {request.command.replace('cmd_', '#')} ожидание
                          </text>
                        </g>
                      );
                    })}
                  </g>
                );
              })()}
              {/* Raft leader replication status: uncommitted + last committed entry (auto-hides) */}
              {!isDead && node.role === 'leader' && node.log.length > 0 && (() => {
                const peers = (node.meta.peers as string[] | undefined) ?? [];
                const totalNodes = peers.length + 1;
                const majority = Math.floor(totalNodes / 2) + 1;
                const lastCommitTime = (node.meta.lastCommitTime as number) ?? 0;
                const hbInterval = (node.meta.heartbeatInterval as number) ?? 100;
                const hasUncommitted = node.log.some(entry => entry.index > node.commitIndex);
                // Hide committed badge after one heartbeat interval (no pending → gone with next heartbeat)
                const commitAge = currentTime - lastCommitTime;
                const showCommitted = hasUncommitted || commitAge < hbInterval;
                const entries: Array<{ cmd: string; replicated: number; committed: boolean; opacity: number }> = [];
                // Show last committed entry until next heartbeat
                if (node.commitIndex >= 0 && showCommitted) {
                  const committedEntry = [...node.log].reverse().find(entry => entry.index === node.commitIndex);
                  if (committedEntry) {
                    let rep = 1;
                    for (const peer of peers) {
                      if ((node.matchIndex.get(peer) ?? -1) >= committedEntry.index) rep++;
                    }
                    const fadeOpacity = hasUncommitted ? 0.85 : Math.max(0.3, 0.85 * (1 - commitAge / hbInterval));
                    entries.push({
                      cmd: committedEntry.command.replace('cmd_', '#'),
                      replicated: rep,
                      committed: true,
                      opacity: fadeOpacity,
                    });
                  }
                }
                // Uncommitted entries
                for (const entry of node.log.filter(logEntry => logEntry.index > node.commitIndex)) {
                  let rep = 1;
                  for (const peer of peers) {
                    if ((node.matchIndex.get(peer) ?? -1) >= entry.index) rep++;
                  }
                  entries.push({
                    cmd: entry.command.replace('cmd_', '#'),
                    replicated: rep, committed: false, opacity: 0.85,
                  });
                }
                if (entries.length === 0) return null;
                const badgeY = pos.y + nodeRadius + 20;
                const lineH = 13;
                const badgeW = 62;
                const visible = entries.slice(-3);
                return (
                  <g>
                    {visible.map((pe, qi) => {
                      const ey = badgeY + lineH * qi;
                      const hasQuorum = pe.replicated >= majority;
                      return (
                        <g key={qi}>
                          <rect x={pos.x - badgeW / 2} y={ey - lineH / 2}
                            width={badgeW} height={lineH} rx={3}
                            fill={hasQuorum ? 'var(--msg-replication)' : 'var(--msg-client)'}
                            opacity={pe.opacity} />
                          <text x={pos.x} y={ey}
                            textAnchor="middle" dominantBaseline="central"
                            fontSize={8} fontWeight="bold" fill="white"
                            opacity={pe.opacity}>
                            {pe.cmd} {pe.replicated}/{totalNodes}{hasQuorum ? ' ✓' : ''}
                          </text>
                        </g>
                      );
                    })}
                  </g>
                );
              })()}
              <text x={pos.x} y={pos.y - 3} textAnchor="middle" dominantBaseline="middle"
                fontSize={fontSize} fontWeight="bold"
                fill={isDead ? 'var(--text-muted)' : 'var(--text-on-node)'}>
                {isDead ? '✕' : roleInfo.label}
              </text>
              <text x={pos.x} y={pos.y + 8} textAnchor="middle" dominantBaseline="middle"
                fontSize={fontSize * 0.75} fill={isDead ? 'var(--text-muted)' : 'var(--text-on-node)'} opacity={0.85}>
                T{node.currentTerm}
              </text>
              <text x={pos.x} y={pos.y + nodeRadius + 10} textAnchor="middle"
                fontSize={fontSize * 0.8} fill="var(--text-secondary)">
                #{parseInt(id.split('_')[1]) + 1}
              </text>
              {node.commitIndex >= 0 && !isDead && (
                <g>
                  <circle cx={pos.x + nodeRadius * 0.7} cy={pos.y - nodeRadius * 0.7} r={6}
                    fill="var(--badge-bg)" stroke="var(--badge-stroke)" strokeWidth={1} />
                  <text x={pos.x + nodeRadius * 0.7} y={pos.y - nodeRadius * 0.7}
                    textAnchor="middle" dominantBaseline="central"
                    fontSize={6} fontWeight="bold" fill="var(--badge-text)">
                    {node.commitIndex + 1}
                  </text>
                </g>
              )}
            </g>
          );
        })}

        {/* Clients with auto-send arc */}
        {clientPositions.map(({ client, x, y }) => {
          const isPending = !!client.pendingCommand;
          const cr = 14;
          // Compute auto-send progress arc
          const nextFire = autoClientTiming?.get(client.id);
          const clientInterval = clients.length * AUTO_CLIENT_INTERVAL;
          let autoProgress = 0;
          if (nextFire !== undefined && clientInterval > 0) {
            const remaining = nextFire - currentTime;
            if (remaining > 0 && remaining <= clientInterval) {
              autoProgress = 1 - remaining / clientInterval;
            } else if (remaining <= 0) {
              autoProgress = 1;
            }
          }
          return (
            <g key={client.id}>
              {/* Auto-send progress arc around client */}
              {autoProgress > 0 && !isPending && (
                <TimeoutArc
                  cx={x} cy={y}
                  r={cr + 3}
                  progress={autoProgress}
                  color="var(--msg-client)"
                  width={2}
                />
              )}
              <rect x={x - cr} y={y - cr * 0.7} width={cr * 2} height={cr * 1.4} rx={4}
                fill={isPending ? 'var(--msg-client)' : 'var(--btn-bg)'}
                stroke={isPending ? 'var(--msg-client)' : 'var(--border-color)'}
                strokeWidth={1.5} opacity={isPending ? 0.9 : 0.6}
              />
              <text x={x} y={y - 1} textAnchor="middle" dominantBaseline="middle"
                fontSize={fontSize * 0.7} fontWeight="bold"
                fill={isPending ? 'var(--text-on-node)' : 'var(--text-secondary)'}>
                {client.id.replace('client_', 'C')}
              </text>
              <text x={x} y={y + 7} textAnchor="middle"
                fontSize={fontSize * 0.6}
                fill={isPending ? 'var(--text-on-node)' : 'var(--text-muted)'}>
                {client.completedCommands > 0 ? `✓${client.completedCommands}` : '—'}
              </text>
            </g>
          );
        })}
      </svg>

      {/* Zoom controls */}
      <div className="zoom-controls">
        <button className="btn btn-xs" onClick={handleZoomOut} title="Уменьшить">−</button>
        <span className="zoom-value">{Math.round(zoom * 100)}%</span>
        <button className="btn btn-xs" onClick={handleZoomIn} title="Увеличить">+</button>
      </div>
    </div>
  );
});
