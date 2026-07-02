import React from 'react';
import { NodeState, NodeId } from '../simulation/types';

interface NodeDetailProps {
  node: NodeState | null;
  onKill?: (id: NodeId) => void;
  onRecover?: (id: NodeId) => void;
  onClose?: () => void;
}

export const NodeDetail: React.FC<NodeDetailProps> = React.memo(({ node, onKill, onRecover, onClose }) => {
  if (!node) {
    return (
      <div className="node-detail empty">
        <p className="hint">Нажмите на узел для просмотра деталей</p>
      </div>
    );
  }

  const isAlive = node.status === 'alive';
  const metaRows = buildMetaRows(node);
  const recentEntries = node.log.slice(-10).reverse();

  return (
    <div className="node-detail">
      <div className="node-detail-header">
        <h4>Узел #{parseInt(node.id.split('_')[1]) + 1}</h4>
        <span className={`status-badge ${node.status}`}>
          {isAlive ? node.role : 'отключён'}
        </span>
        <button className="btn btn-xs node-detail-close" onClick={onClose} title="Закрыть">✕</button>
      </div>

      <div className="node-detail-actions">
        {isAlive ? (
          <button className="btn btn-sm btn-danger" onClick={() => onKill?.(node.id)}>
            Отключить
          </button>
        ) : (
          <button className="btn btn-sm btn-success" onClick={() => onRecover?.(node.id)}>
            Восстановить
          </button>
        )}
      </div>

      <div className="node-detail-info">
        <div className="info-row">
          <span className="info-label">Терм:</span>
          <span className="info-value">{node.currentTerm}</span>
        </div>
        <div className="info-row">
          <span className="info-label">Голос за:</span>
          <span className="info-value">{node.votedFor ?? '—'}</span>
        </div>
        <div className="info-row">
          <span className="info-label">Commit index:</span>
          <span className="info-value">{node.commitIndex}</span>
        </div>
        <div className="info-row">
          <span className="info-label">Last applied:</span>
          <span className="info-value">{node.lastApplied}</span>
        </div>
        <div className="info-row">
          <span className="info-label">Записей в логе:</span>
          <span className="info-value">{node.log.length} с #{node.logBaseIndex}</span>
        </div>
      </div>

      {metaRows.length > 0 && (
        <div className="node-detail-section">
          <h5>Состояние алгоритма</h5>
          <div className="node-detail-info">
            {metaRows.map(row => (
              <div className="info-row" key={row.label}>
                <span className="info-label">{row.label}:</span>
                <span className="info-value">{row.value}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {recentEntries.length > 0 && (
        <div className="node-log">
          <h5>Лог</h5>
          <div className="log-entries">
            {recentEntries.map((entry) => (
              <div
                key={entry.entryId}
                className={`log-entry ${entry.committed ? 'committed' : 'pending'} ${entry.index <= node.lastApplied ? 'applied' : ''}`}
              >
                <div className="log-entry-main">
                  <span className="log-index">#{entry.index}</span>
                  <span className="log-term">T{entry.term}</span>
                  <span className="log-command">{entry.command}</span>
                </div>
                {formatEntryMeta(entry) && (
                  <div className="log-entry-meta">{formatEntryMeta(entry)}</div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
});

function buildMetaRows(node: NodeState): Array<{ label: string; value: string | number }> {
  const rows: Array<{ label: string; value: string | number }> = [];
  const { meta } = node;

  const knownLeader = asString(meta.knownLeader);
  if (knownLeader) rows.push({ label: 'Лидер', value: knownLeader });

  const phase = asString(meta.phase);
  if (phase) rows.push({ label: 'Фаза', value: phase });

  const epoch = asNumber(meta.epoch);
  if (epoch !== null) rows.push({ label: 'Epoch', value: epoch });

  const counter = asNumber(meta.counter);
  if (counter !== null) rows.push({ label: 'ZXID счётчик', value: counter });

  const ballot = asNumber(meta.leaderBallot) ?? asNumber(meta.proposalNumber);
  if (ballot !== null) rows.push({ label: 'Ballot', value: ballot });

  const currentSlot = asNumber(meta.currentSlot);
  if (currentSlot !== null) rows.push({ label: 'Текущий слот', value: currentSlot });

  const nextSlot = asNumber(meta.nextProposalSlot);
  if (nextSlot !== null) rows.push({ label: 'Следующий слот', value: nextSlot });

  const minProposal = asNumber(meta.minProposal);
  if (minProposal !== null) rows.push({ label: 'Min proposal', value: minProposal });

  const acceptedProposal = asNumber(meta.acceptedProposal);
  if (acceptedProposal !== null && acceptedProposal >= 0) {
    rows.push({ label: 'Accepted proposal', value: acceptedProposal });
  }

  const activeInstance = asString(meta.activeInstance);
  if (activeInstance) rows.push({ label: 'Active instance', value: activeInstance });

  const maxSeq = asNumber(meta.maxSeq);
  if (maxSeq !== null) rows.push({ label: 'Max seq', value: maxSeq });

  const instances = meta.instances;
  if (instances && typeof instances === 'object') {
    const values = Object.values(instances as Record<string, { status?: string }>);
    const committed = values.filter(instance => instance.status === 'committed').length;
    const inflight = values.filter(instance => instance.status !== 'committed').length;
    rows.push({ label: 'Инстансы', value: `${committed} committed / ${inflight} active` });
  }

  return rows;
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function formatEntryMeta(entry: NodeState['log'][number]): string | null {
  const parts: string[] = [];
  if (entry.zxid) parts.push(`zxid ${entry.zxid}`);
  if (entry.instanceKey) parts.push(`instance ${entry.instanceKey}`);
  if (entry.requestId !== entry.entryId) parts.push(`req ${entry.requestId}`);
  return parts.length > 0 ? parts.join(' • ') : null;
}
