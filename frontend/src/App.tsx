import { useEffect, useMemo, useState } from 'react';
import { SimulationPanel } from './ui/SimulationPanel';
import { ThemeToggle } from './ui/ThemeToggle';
import { useTheme } from './hooks/useTheme';
import { AlgorithmType } from './hooks/useSimulation';
import { NetworkProfile } from './simulation/constants';
import { getComparisonScenarioPresets } from './simulation/scenarios';
import './App.css';

interface PanelConfig {
  id: string;
  algorithm: AlgorithmType;
  nodeCount: number;
  networkProfile: NetworkProfile;
  clientCount: number;
}

function App() {
  const { preference, cycleTheme } = useTheme();

  const [panels, setPanels] = useState<PanelConfig[]>([
    { id: 'a', algorithm: 'raft', nodeCount: 3, networkProfile: 'wan', clientCount: 2 },
    { id: 'b', algorithm: 'paxos', nodeCount: 5, networkProfile: 'wan', clientCount: 2 },
  ]);
  const [comparisonScenarioId, setComparisonScenarioId] = useState(() => getScenarioIdFromUrl() ?? 'compare-failover');
  const [comparisonRunToken, setComparisonRunToken] = useState(0);
  const [comparisonClearToken, setComparisonClearToken] = useState(0);

  const comparisonPresets = useMemo(
    () => getComparisonScenarioPresets(Math.max(...panels.map(panel => panel.nodeCount))),
    [panels],
  );
  const effectiveComparisonScenarioId = comparisonPresets.some(preset => preset.id === comparisonScenarioId)
    ? comparisonScenarioId
    : (comparisonPresets[0]?.id ?? 'compare-failover');

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const url = new URL(window.location.href);
    url.searchParams.set('scenario', effectiveComparisonScenarioId);
    window.history.replaceState({}, '', url);
  }, [effectiveComparisonScenarioId]);

  const updatePanel = (index: number, update: { algorithm: AlgorithmType; nodeCount: number }) => {
    setPanels(prev => prev.map((p, i) =>
      i === index ? { ...p, ...update } : p
    ));
  };

  const addPanel = () => {
    if (panels.length >= 3) return;
    const id = String.fromCharCode(97 + panels.length);
    setPanels(prev => [...prev, { id, algorithm: 'paxos', nodeCount: 5, networkProfile: 'wan', clientCount: 2 }]);
  };

  const removePanel = (index: number) => {
    if (panels.length <= 1) return;
    setPanels(prev => prev.filter((_, i) => i !== index));
  };

  const runComparisonScenario = () => {
    setComparisonRunToken(token => token + 1);
  };

  const clearComparisonScenario = () => {
    setComparisonClearToken(token => token + 1);
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1 className="app-title">Consensus Landscape</h1>
        <p className="app-subtitle">Интерактивное сравнение алгоритмов консенсуса</p>
        <div className="header-actions">
          <div className="header-scenarios">
            <select
              value={effectiveComparisonScenarioId}
              onChange={e => setComparisonScenarioId(e.target.value)}
              className="select-scenario header-select"
            >
              {comparisonPresets.map(preset => (
                <option key={preset.id} value={preset.id}>{preset.label}</option>
              ))}
            </select>
            <button className="btn btn-sm" onClick={runComparisonScenario}>
              Во все панели
            </button>
            <button className="btn btn-sm" onClick={clearComparisonScenario}>
              Очистить везде
            </button>
          </div>
          <div className="header-scenarios-meta">
            <span className="header-scenarios-hint">Сценарий из шапки запускается сразу во всех панелях</span>
            <span className="header-scenarios-legend">✕ отказ • ↑ recovery • ⇄ split • ⇆ heal • ✎ write</span>
          </div>
          {panels.length < 3 && (
            <button className="btn btn-sm" onClick={addPanel}>
              + Панель
            </button>
          )}
          <ThemeToggle preference={preference} onToggle={cycleTheme} />
        </div>
      </header>

      <main className={`panels-container panels-${panels.length}`}>
        {panels.map((panel, i) => (
          <div key={panel.id} className="panel-wrapper">
            {panels.length > 1 && (
              <button
                className="btn btn-icon panel-close"
                onClick={() => removePanel(i)}
                title="Убрать панель"
              >
                ✕
              </button>
            )}
            <SimulationPanel
              id={panel.id}
              algorithmType={panel.algorithm}
              nodeCount={panel.nodeCount}
              networkProfile={panel.networkProfile}
              clientCount={panel.clientCount}
              broadcastScenarioId={effectiveComparisonScenarioId}
              broadcastScenarioToken={comparisonRunToken}
              broadcastClearToken={comparisonClearToken}
              onConfigChange={update => updatePanel(i, update)}
            />
          </div>
        ))}
      </main>

      <footer className="app-footer">
        <a href="/docs/" className="footer-link" target="_blank" rel="noopener">Документация</a>
        <span className="copyright">Consensus Landscape &copy; {new Date().getFullYear()}</span>
        <span className="build-label" title={`Build ${__BUILD_HASH__} (${__BUILD_DATE__})`}>{__BUILD_REF__}:{__BUILD_HASH__}</span>
      </footer>
    </div>
  );
}

export default App;

function getScenarioIdFromUrl(): string | null {
  if (typeof window === 'undefined') return null;
  const value = new URLSearchParams(window.location.search).get('scenario');
  return value && value.length > 0 ? value : null;
}
