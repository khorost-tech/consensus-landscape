import type { AlgorithmType } from '../hooks/useSimulation';
import type { NodeId, Scenario } from './types';

export interface ScenarioAction {
  timeOffset: number;
  type: 'kill_node' | 'recover_node' | 'partition' | 'heal_partition' | 'client_write';
  params: {
    nodeId?: NodeId;
    command?: string;
    clientId?: string;
    targetNode?: NodeId;
    partitions?: NodeId[][];
  };
}

export interface ScenarioPreset {
  id: string;
  label: string;
  description: string;
  actions: ScenarioAction[];
}

type ScenarioFactory = (nodeCount: number) => ScenarioPreset[];

function boundedNodeIds(nodeCount: number, count: number): NodeId[] {
  return Array.from({ length: Math.min(nodeCount, count) }, (_, index) => `node_${index}`);
}

function majorityMinorityPartition(nodeCount: number): NodeId[][] {
  const majoritySize = Math.max(2, Math.ceil(nodeCount / 2));
  const nodes = boundedNodeIds(nodeCount, nodeCount);
  return [nodes.slice(0, majoritySize), nodes.slice(majoritySize)];
}

const factories: Record<AlgorithmType, ScenarioFactory> = {
  raft: (nodeCount) => [
    {
      id: 'raft-leader-failover',
      label: 'Смена лидера',
      description: 'Лидер падает, кластер переизбирается и затем догоняет восстановленный узел.',
      actions: [
        { timeOffset: 250, type: 'client_write', params: { command: 'set x=1', clientId: 'client_0' } },
        { timeOffset: 650, type: 'kill_node', params: { nodeId: 'node_0' } },
        { timeOffset: 900, type: 'client_write', params: { command: 'set x=2', clientId: 'client_0' } },
        { timeOffset: 1350, type: 'recover_node', params: { nodeId: 'node_0' } },
        { timeOffset: 1600, type: 'client_write', params: { command: 'set x=3', clientId: 'client_0' } },
      ],
    },
    {
      id: 'raft-partition-recovery',
      label: 'Partition и heal',
      description: 'Сеть делится на majority/minority, запись проходит только в majority, затем отставшие узлы догоняют кластер.',
      actions: [
        { timeOffset: 300, type: 'partition', params: { partitions: majorityMinorityPartition(nodeCount) } },
        { timeOffset: 500, type: 'client_write', params: { command: 'set y=1', clientId: 'client_0', targetNode: 'node_0' } },
        { timeOffset: 1100, type: 'heal_partition', params: {} },
        { timeOffset: 1350, type: 'client_write', params: { command: 'set y=2', clientId: 'client_0' } },
      ],
    },
  ],
  paxos: () => [
    {
      id: 'paxos-repeated-values',
      label: 'Повторяющиеся команды',
      description: 'Одинаковые команды должны закоммититься как разные слоты, а не схлопнуться в одну операцию.',
      actions: [
        { timeOffset: 50, type: 'client_write', params: { command: 'set k=1', clientId: 'client_0', targetNode: 'node_0' } },
        { timeOffset: 220, type: 'client_write', params: { command: 'set k=1', clientId: 'client_0', targetNode: 'node_1' } },
        { timeOffset: 420, type: 'client_write', params: { command: 'set k=1', clientId: 'client_0', targetNode: 'node_2' } },
      ],
    },
    {
      id: 'paxos-dueling-proposers',
      label: 'Конкурирующие proposers',
      description: 'Два клиента одновременно бьют в разные узлы и вызывают Prepare/NACK конкуренцию.',
      actions: [
        { timeOffset: 80, type: 'client_write', params: { command: 'alpha', clientId: 'client_0', targetNode: 'node_0' } },
        { timeOffset: 80, type: 'client_write', params: { command: 'beta', clientId: 'client_1', targetNode: 'node_1' } },
        { timeOffset: 320, type: 'client_write', params: { command: 'gamma', clientId: 'client_0', targetNode: 'node_2' } },
      ],
    },
  ],
  'multi-paxos': (nodeCount) => [
    {
      id: 'multi-paxos-stable-leader',
      label: 'Stable leader path',
      description: 'После выбора лидера одинаковые команды проходят последовательно по слотам.',
      actions: [
        { timeOffset: 260, type: 'client_write', params: { command: 'append a', clientId: 'client_0' } },
        { timeOffset: 420, type: 'client_write', params: { command: 'append a', clientId: 'client_0' } },
        { timeOffset: 580, type: 'client_write', params: { command: 'append b', clientId: 'client_0' } },
      ],
    },
    {
      id: 'multi-paxos-minority-partition',
      label: 'Лидер против minority',
      description: 'Minority-partition срезает стабильного лидера от кворума, после heal лидерство восстанавливается.',
      actions: [
        { timeOffset: 250, type: 'partition', params: { partitions: majorityMinorityPartition(nodeCount) } },
        { timeOffset: 450, type: 'client_write', params: { command: 'write one', clientId: 'client_0', targetNode: 'node_0' } },
        { timeOffset: 1050, type: 'heal_partition', params: {} },
        { timeOffset: 1300, type: 'client_write', params: { command: 'write two', clientId: 'client_0' } },
      ],
    },
  ],
  zab: () => [
    {
      id: 'zab-follower-sync',
      label: 'Follower sync',
      description: 'Follower падает, лидер продолжает broadcast, затем recovered узел получает `zab_sync` и догоняет лог.',
      actions: [
        { timeOffset: 250, type: 'kill_node', params: { nodeId: 'node_2' } },
        { timeOffset: 420, type: 'client_write', params: { command: 'zab one', clientId: 'client_0', targetNode: 'node_0' } },
        { timeOffset: 620, type: 'client_write', params: { command: 'zab two', clientId: 'client_0', targetNode: 'node_0' } },
        { timeOffset: 920, type: 'recover_node', params: { nodeId: 'node_2' } },
      ],
    },
    {
      id: 'zab-epoch-turnover',
      label: 'Смена epoch',
      description: 'Старый лидер падает, происходит новая election/discovery фаза, затем запись идёт уже в новом epoch.',
      actions: [
        { timeOffset: 240, type: 'kill_node', params: { nodeId: 'node_0' } },
        { timeOffset: 760, type: 'client_write', params: { command: 'after re-election', clientId: 'client_0' } },
        { timeOffset: 1150, type: 'recover_node', params: { nodeId: 'node_0' } },
      ],
    },
  ],
  epaxos: () => [
    {
      id: 'epaxos-fast-vs-slow',
      label: 'Fast path vs slow path',
      description: 'Сначала независимые запросы проходят быстро, затем конфликтные запросы заставляют перейти в slow path.',
      actions: [
        { timeOffset: 60, type: 'client_write', params: { command: 'left', clientId: 'client_0', targetNode: 'node_0' } },
        { timeOffset: 60, type: 'client_write', params: { command: 'right', clientId: 'client_1', targetNode: 'node_1' } },
        { timeOffset: 320, type: 'client_write', params: { command: 'conflict', clientId: 'client_0', targetNode: 'node_0' } },
        { timeOffset: 320, type: 'client_write', params: { command: 'conflict', clientId: 'client_1', targetNode: 'node_1' } },
      ],
    },
    {
      id: 'epaxos-replica-loss',
      label: 'Потеря реплики',
      description: 'Одна реплика пропускает коммиты, а после восстановления получает catch-up по committed instance.',
      actions: [
        { timeOffset: 120, type: 'kill_node', params: { nodeId: 'node_2' } },
        { timeOffset: 240, type: 'client_write', params: { command: 'ep one', clientId: 'client_0', targetNode: 'node_0' } },
        { timeOffset: 240, type: 'client_write', params: { command: 'ep two', clientId: 'client_1', targetNode: 'node_1' } },
        { timeOffset: 740, type: 'recover_node', params: { nodeId: 'node_2' } },
      ],
    },
  ],
};

export function getScenarioPresets(algorithm: AlgorithmType, nodeCount: number): ScenarioPreset[] {
  return factories[algorithm](nodeCount);
}

const comparisonFactory: ScenarioFactory = (nodeCount) => [
  {
    id: 'compare-failover',
    label: 'Сбой и восстановление',
    description: 'Один узел падает, затем возвращается, а кластер продолжает принимать новые записи.',
    actions: [
      { timeOffset: 220, type: 'client_write', params: { command: 'warmup', clientId: 'client_0' } },
      { timeOffset: 520, type: 'kill_node', params: { nodeId: 'node_0' } },
      { timeOffset: 860, type: 'client_write', params: { command: 'during failover', clientId: 'client_0' } },
      { timeOffset: 1320, type: 'recover_node', params: { nodeId: 'node_0' } },
      { timeOffset: 1560, type: 'client_write', params: { command: 'after recovery', clientId: 'client_0' } },
    ],
  },
  {
    id: 'compare-partition-heal',
    label: 'Partition и heal',
    description: 'Сеть делится на majority/minority, затем разделение устраняется и кластер восстанавливает прогресс.',
    actions: [
      { timeOffset: 280, type: 'partition', params: { partitions: majorityMinorityPartition(nodeCount) } },
      { timeOffset: 520, type: 'client_write', params: { command: 'under partition', clientId: 'client_0', targetNode: 'node_0' } },
      { timeOffset: 1180, type: 'heal_partition', params: {} },
      { timeOffset: 1450, type: 'client_write', params: { command: 'after heal', clientId: 'client_0' } },
    ],
  },
  {
    id: 'compare-concurrent-writes',
    label: 'Параллельные записи',
    description: 'Два клиента одновременно создают конкурентные записи, чтобы было видно различие fast/slow path и quorum path.',
    actions: [
      { timeOffset: 120, type: 'client_write', params: { command: 'alpha', clientId: 'client_0', targetNode: 'node_0' } },
      { timeOffset: 120, type: 'client_write', params: { command: 'beta', clientId: 'client_1', targetNode: nodeCount > 1 ? 'node_1' : 'node_0' } },
      { timeOffset: 420, type: 'client_write', params: { command: 'gamma', clientId: 'client_0' } },
    ],
  },
];

export function getComparisonScenarioPresets(nodeCount: number): ScenarioPreset[] {
  return comparisonFactory(nodeCount);
}

export function getComparisonScenarioPreset(id: string, nodeCount: number): ScenarioPreset | null {
  return getComparisonScenarioPresets(nodeCount).find(preset => preset.id === id) ?? null;
}

export function presetToScenario(preset: ScenarioPreset, nodeCount: number): Scenario {
  return {
    id: preset.id,
    name: preset.label,
    description: preset.description,
    clusterConfig: {
      nodeCount,
      clientCount: 2,
      observerIds: [],
      networkConfig: {
        minDelay: 0,
        maxDelay: 0,
        packetLossRate: 0,
        partitions: [],
      },
      electionTimeoutMin: 0,
      electionTimeoutMax: 0,
      heartbeatInterval: 0,
    },
    events: preset.actions.map(action => ({
      time: action.timeOffset,
      type: action.type,
      params: action.params,
    })),
  };
}
