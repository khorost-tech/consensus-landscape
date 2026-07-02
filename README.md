# Consensus Landscape

[![Deploy](https://github.com/khorost/consensus-landscape/actions/workflows/deploy.yml/badge.svg)](https://github.com/khorost/consensus-landscape/actions/workflows/deploy.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.9-blue.svg)](https://www.typescriptlang.org/)

Интерактивный симулятор алгоритмов консенсуса (Raft, Paxos, Multi-Paxos, Zab, EPaxos) для визуального сравнения их поведения. Учебный инструмент для студентов.

Симулятор ориентирован на **учебную точность**, а не на production-совместимость. `Raft`, `Paxos`, `Multi-Paxos` и `Zab` реализованы как достаточно корректные учебные модели. `EPaxos` реализован как продвинутая учебная аппроксимация: в ней сохранены ключевые идеи `fast path`, `slow path` и зависимостей между instance, но полная paper-level семантика оригинального алгоритма сознательно упрощена.

**[Открыть симулятор →](https://khorost.github.io/consensus-landscape/)** | **[Документация →](https://khorost.github.io/consensus-landscape/docs/)**

![Consensus Landscape — параллельное сравнение Raft и Paxos](docs/public/img/simulator-screenshot.png)
