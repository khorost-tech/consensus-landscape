import React, { useMemo } from 'react';
import { SimulationMetrics, ScenarioMarker } from '../simulation/types';

interface LatencyChartProps {
  metrics: SimulationMetrics;
  scenarioMarkers?: ScenarioMarker[];
  currentTime: number;
  width?: number;
  height?: number;
}

const PADDING = { top: 8, right: 8, bottom: 18, left: 32 };

export const LatencyChart: React.FC<LatencyChartProps> = React.memo(({
  metrics, scenarioMarkers = [], currentTime, width = 400, height = 80,
}) => {
  const { commitTimestamps, commitLatencies, leaderChangeTimestamps, nodeEvents, conflictTimestamps, statusZones } = metrics;

  const chartW = width - PADDING.left - PADDING.right;
  const chartH = height - PADDING.top - PADDING.bottom;

  const hasAnyActivity = commitTimestamps.length > 0
    || conflictTimestamps.length > 0
    || statusZones.length > 0
    || scenarioMarkers.length > 0;

  const data = useMemo(() => {
    if (!hasAnyActivity && currentTime < 100) return null;

    const tMin = 0;
    const tMax = Math.max(currentTime, commitTimestamps.length > 0 ? commitTimestamps[commitTimestamps.length - 1] + 100 : currentTime);
    const lMax = commitLatencies.length > 0 ? Math.max(20, ...commitLatencies) * 1.1 : 100;

    const points = commitTimestamps.map((t, i) => ({
      x: PADDING.left + ((t - tMin) / (tMax - tMin)) * chartW,
      y: PADDING.top + chartH - (commitLatencies[i] / lMax) * chartH,
      latency: commitLatencies[i],
    }));

    // Build SVG polyline
    const polyline = points.map(p => `${p.x},${p.y}`).join(' ');

    // Leader change lines
    const leaderLines = leaderChangeTimestamps
      .filter(t => t >= tMin)
      .map(t => PADDING.left + ((t - tMin) / (tMax - tMin)) * chartW);

    // Node event lines
    const failureLines = nodeEvents
      .filter(e => e.type === 'failure' && e.time >= tMin)
      .map(e => ({
        x: PADDING.left + ((e.time - tMin) / (tMax - tMin)) * chartW,
        nodeId: e.nodeId,
      }));

    const recoveryLines = nodeEvents
      .filter(e => e.type === 'recovery' && e.time >= tMin)
      .map(e => ({
        x: PADDING.left + ((e.time - tMin) / (tMax - tMin)) * chartW,
        nodeId: e.nodeId,
      }));

    // Conflict markers (NACK) — placed at y=0 (bottom of chart)
    const conflictDots = conflictTimestamps
      .filter(t => t >= tMin)
      .map(t => PADDING.left + ((t - tMin) / (tMax - tMin)) * chartW);

    // Status zones (no quorum = red, electing = blue)
    const zones = statusZones.map(z => ({
      x1: PADDING.left + ((z.start - tMin) / (tMax - tMin)) * chartW,
      x2: PADDING.left + ((Math.min(z.end, tMax) - tMin) / (tMax - tMin)) * chartW,
      type: z.type,
    }));

    const scenarioLines = scenarioMarkers
      .filter(marker => marker.time >= tMin)
      .map(marker => ({
        ...marker,
        x: PADDING.left + ((marker.time - tMin) / (tMax - tMin)) * chartW,
      }));

    return { points, polyline, leaderLines, failureLines, recoveryLines, conflictDots, zones, scenarioLines, lMax, tMax, tMin };
  }, [commitTimestamps, commitLatencies, leaderChangeTimestamps, nodeEvents, conflictTimestamps, statusZones, scenarioMarkers, currentTime, chartW, chartH, hasAnyActivity]);

  if (!data) {
    return (
      <div className="latency-chart empty">
        <span className="hint">Ожидание данных...</span>
      </div>
    );
  }

  // Y axis labels
  const yLabels = [0, Math.round(data.lMax / 2), Math.round(data.lMax)];

  return (
    <div className="latency-chart">
      <svg viewBox={`0 0 ${width} ${height}`} width="100%" height={height}>
        {/* Status zones background */}
        {data.zones.map((z, i) => (
          <rect key={`zone-${i}`}
            x={z.x1} y={PADDING.top}
            width={Math.max(1, z.x2 - z.x1)} height={chartH}
            fill={z.type === 'no_quorum' ? 'var(--msg-nack)' : 'var(--node-follower)'}
            opacity={0.12}
          />
        ))}

        {/* Grid lines */}
        {yLabels.map(v => {
          const y = PADDING.top + chartH - (v / data.lMax) * chartH;
          return (
            <g key={`grid-${v}`}>
              <line x1={PADDING.left} y1={y} x2={width - PADDING.right} y2={y}
                stroke="var(--border-color)" strokeWidth={0.5} />
              <text x={PADDING.left - 3} y={y} textAnchor="end" dominantBaseline="middle"
                fontSize={7} fill="var(--text-muted)">
                {v}
              </text>
            </g>
          );
        })}

        {/* Node failure lines — red dashed */}
        {data.failureLines.map((f, i) => (
          <g key={`fail-${i}`}>
            <line x1={f.x} y1={PADDING.top} x2={f.x} y2={PADDING.top + chartH}
              stroke="var(--msg-nack)" strokeWidth={1} strokeDasharray="3 2" opacity={0.7} />
            <text x={f.x} y={PADDING.top + chartH + 10} textAnchor="middle"
              fontSize={6} fill="var(--msg-nack)">
              ✕{parseInt(f.nodeId.split('_')[1]) + 1}
            </text>
          </g>
        ))}

        {/* Node recovery lines — green dashed */}
        {data.recoveryLines.map((r, i) => (
          <g key={`recv-${i}`}>
            <line x1={r.x} y1={PADDING.top} x2={r.x} y2={PADDING.top + chartH}
              stroke="var(--node-leader)" strokeWidth={1} strokeDasharray="3 2" opacity={0.5} />
            <text x={r.x} y={PADDING.top + chartH + 10} textAnchor="middle"
              fontSize={6} fill="var(--node-leader)">
              ↑{parseInt(r.nodeId.split('_')[1]) + 1}
            </text>
          </g>
        ))}

        {/* Leader change lines — purple dotted */}
        {data.leaderLines.map((x, i) => (
          <line key={`leader-${i}`}
            x1={x} y1={PADDING.top} x2={x} y2={PADDING.top + chartH}
            stroke="var(--msg-vote)" strokeWidth={0.8} strokeDasharray="2 3" opacity={0.5}
          />
        ))}

        {/* Scenario markers */}
        {data.scenarioLines.map((marker, i) => (
          <g key={`scenario-${i}`}>
            <title>{marker.detail ?? marker.label}</title>
            <line
              x1={marker.x}
              y1={PADDING.top}
              x2={marker.x}
              y2={PADDING.top + chartH}
              stroke={scenarioColor(marker.type)}
              strokeWidth={0.9}
              strokeDasharray="1.5 2.5"
              opacity={0.7}
            />
            <text
              x={marker.x}
              y={PADDING.top + 6}
              textAnchor="middle"
              fontSize={6}
              fill={scenarioColor(marker.type)}
            >
              {marker.label}
            </text>
          </g>
        ))}

        {/* Latency line */}
        <polyline
          points={data.polyline}
          fill="none"
          stroke="var(--btn-primary-bg)"
          strokeWidth={1.5}
          strokeLinejoin="round"
        />

        {/* Conflict dots (NACK) — red crosses on x-axis */}
        {data.conflictDots.map((x, i) => (
          <g key={`conflict-${i}`}>
            <circle cx={x} cy={PADDING.top + chartH - 2} r={2.5}
              fill="var(--msg-nack)" opacity={0.8} />
            <title>Конфликт (NACK)</title>
          </g>
        ))}

        {/* Data points */}
        {data.points.map((p, i) => (
          <circle key={i} cx={p.x} cy={p.y} r={2}
            fill="var(--btn-primary-bg)" opacity={0.7}>
            <title>{p.latency.toFixed(0)} мс</title>
          </circle>
        ))}

        {/* Axis labels */}
        <text x={PADDING.left} y={height - 2} fontSize={7} fill="var(--text-muted)">
          Латентность (мс)
        </text>
      </svg>
    </div>
  );
});

function scenarioColor(type: ScenarioMarker['type']): string {
  switch (type) {
    case 'kill_node':
      return 'var(--msg-nack)';
    case 'recover_node':
      return 'var(--node-leader)';
    case 'partition':
      return 'var(--msg-vote)';
    case 'heal_partition':
      return 'var(--node-follower)';
    case 'client_write':
      return 'var(--msg-client)';
  }
}
