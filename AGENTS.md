# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project

**consensus-landscape** — интерактивный симулятор алгоритмов консенсуса (Paxos, Raft) для визуального сравнения их поведения. Учебный инструмент для студентов. MIT лицензия.

Ключевая фича: параллельное сравнение 2-3 симуляций с разными алгоритмами/конфигурациями на одном экране.

## Build & Dev Commands

All commands run from `frontend/` directory:

```bash
cd frontend
npm install          # install dependencies
npm run dev          # dev server (Vite, http://localhost:5173)
npm run build        # type-check + production build
npm run preview      # preview production build
npm run lint         # ESLint
```

## Architecture

Monorepo structure: `frontend/` contains the SPA (React + TypeScript + Vite). Root is reserved for future backend.

### Simulation Engine (`frontend/src/simulation/`)
- Pure TypeScript, no React dependency — can run headlessly
- Discrete-event simulation with virtual time and seeded PRNG for determinism
- `engine.ts` — event queue, virtual clock, processes events and delegates to algorithm
- `network.ts` — network model (configurable delays, packet loss, partitions)
- `types.ts` — all shared types (NodeState, Message, SimEvent, ClusterConfig, etc.)
- `algorithms/interface.ts` — `ConsensusAlgorithm` interface that all algorithms implement
- `algorithms/raft.ts` — Raft: leader election, log replication, commit advancement
- `algorithms/paxos.ts` — Basic Paxos: prepare/promise, accept/accepted phases

### Visualization (`frontend/src/visualization/`)
- SVG-based, React components
- `ClusterView.tsx` — nodes arranged in circle, animated messages along bezier curves
- `TimelineControl.tsx` — play/pause/step/reset, speed selector (0.25x–10x)
- `NodeDetail.tsx` — selected node details: role, term, log entries, kill/recover actions

### UI (`frontend/src/ui/`)
- `SimulationPanel.tsx` — one simulation instance (algorithm selector + cluster view + controls)
- `ThemeToggle.tsx` — light/dark/system theme switcher

### Hooks (`frontend/src/hooks/`)
- `useSimulation.ts` — manages engine lifecycle, connects rAF loop to engine, exposes state
- `useTheme.ts` — system theme detection + manual override, persists to localStorage

### Key Design Decisions
- Engine is event-driven: visualization samples state via requestAnimationFrame, engine can process events faster than real time
- Nodes colored by role: blue=follower, yellow=candidate, green=leader, gray=dead
- Messages animated as dots along curved paths between nodes
- Up to 10 nodes per simulation, up to 3 parallel panels
- UI labels in Russian (target audience: Russian-speaking students), code in English
- Theme: auto-detects system preference, cycles through system→light→dark
