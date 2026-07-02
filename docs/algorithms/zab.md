# Zab (ZooKeeper Atomic Broadcast)

![Zab — atomic broadcast с лидером](/img/zab-broadcast.png)

## Обзор

Zab — протокол atomic broadcast, разработанный для Apache ZooKeeper (2011). Обеспечивает **total order broadcast** — все узлы видят сообщения в одинаковом порядке. Разработан независимо от Raft, но имеет похожую архитектуру с лидером.

**Ключевые особенности:**
- Три явные фазы: Election → Synchronization → Broadcast
- Версионирование по `epoch` (аналог `term` в Raft)
- Гарантия FIFO-порядка при смене лидеров (causal order)
- Трёхшаговый коммит: Proposal → Ack → Commit

## Роли узлов

```mermaid
stateDiagram-v2
    [*] --> Looking
    Looking --> Leading : Выиграл выборы
    Looking --> Following : Другой узел победил
    Leading --> Looking : Потеря кворума / больший epoch
    Following --> Looking : Leader timeout
```

| Роль | Цвет в симуляторе | Метка | Поведение |
|------|-------------------|-------|-----------|
| **Looking** | 🟡 жёлтый | E | Участвует в выборах; голосует за лучшего кандидата |
| **Following** | 🔵 синий | ZF | Принимает proposals, отправляет ack; ожидает heartbeat |
| **Leading** | 🟢 зелёный | ZL | Принимает клиентские запросы, рассылает proposals |

## Фаза 1: Election (Выборы)

Каждый узел голосует за кандидата с **наивысшим zxid** (epoch, counter). При равенстве — побеждает узел с большим ID.

```mermaid
sequenceDiagram
    participant N0 as Node 0 (Looking)
    participant N1 as Node 1 (Looking)
    participant N2 as Node 2 (Looking)

    Note over N0,N2: Все узлы в состоянии Looking
    N0->>N1: Vote(leader=node_0, epoch=0)
    N0->>N2: Vote(leader=node_0, epoch=0)
    N1->>N0: Vote(leader=node_1, epoch=0)
    N2->>N0: Vote(leader=node_2, epoch=0)
    Note over N0: node_2 > node_0 → обновляет голос
    N0->>N1: Vote(leader=node_2, epoch=0)
    N0->>N2: Vote(leader=node_2, epoch=0)
    Note over N2: Кворум за node_2 → Leading!
    Note over N0,N1: → Following
```

### Правило обновления голоса

Узел обновляет свой голос, если входящее предложение **лучше** текущего:

```
1. Больший epoch                         → обновить
2. Тот же epoch, больший counter          → обновить
3. Тот же epoch и counter, больший nodeId → обновить
```

## Фаза 2: Synchronization (Синхронизация)

Новый лидер приводит follower-ов в актуальное состояние перед началом обработки запросов:

```mermaid
sequenceDiagram
    participant L as Leader (node_2)
    participant F1 as Follower (node_0)
    participant F2 as Follower (node_1)

    Note over L: epoch++ → новая эпоха
    F1->>L: FollowerInfo(lastEpoch, logLength)
    F2->>L: FollowerInfo(lastEpoch, logLength)
    L->>F1: Sync(пропущенные записи)
    L->>F1: NewLeader(epoch)
    L->>F2: NewLeader(epoch)
    F1->>L: AckNewLeader ✓
    F2->>L: AckNewLeader ✓
    Note over L: Кворум ack → Broadcast!
```

Во время синхронизации:
- Follower-ы отправляют `FollowerInfo` с информацией о своём последнем состоянии
- Лидер отправляет `Sync` сообщения с пропущенными committed записями
- Лидер отправляет `NewLeader` с новым epoch
- После получения кворума `AckNewLeader` — переход к Broadcast

## Фаза 3: Broadcast (Обработка запросов)

Трёхшаговый коммит: Proposal → Ack → Commit.

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2

    C->>L: "set x=1"
    Note over L: Создаёт proposal (epoch:counter)
    L->>F1: Proposal(1:1, "set x=1")
    L->>F2: Proposal(1:1, "set x=1")
    F1->>L: Ack(1:1) ✓
    Note over L: Кворум ack → COMMIT
    L->>F1: Commit(1:1)
    L->>F2: Commit(1:1)
    L-->>C: Committed
```

### Отличие от Raft

В Raft коммит piggyback-ится в следующем AppendEntries/heartbeat. В Zab Commit — **явное отдельное сообщение**, что делает протокол трёхшаговым:

| Шаг | Zab | Raft |
|-----|-----|------|
| 1 | Proposal → Follower | AppendEntries → Follower |
| 2 | Ack → Leader | Response → Leader |
| 3 | **Commit → Follower** | _(piggybacked в следующем heartbeat)_ |

## Heartbeats

Лидер рассылает `heartbeat`-сообщения для поддержания лидерства. При пропуске `heartbeat` follower переходит в `Looking` и начинает новые выборы.

## Обработка отказов

### Потеря лидера

1. Heartbeats прекращаются → follower-ы переходят в Looking
2. Начинаются новые выборы с учётом текущего epoch и counter
3. Новый лидер инкрементирует epoch
4. Синхронизация → Broadcast

### Восстановление узла

При восстановлении узел получает `Sync` сообщения с пропущенными committed записями от лучшего живого peer-а.

## Zxid: двумерная версия

Вместо одномерного `term`/`ballot` Zab использует двумерный идентификатор транзакции:

```
zxid = (epoch, counter)
```

- **epoch** — инкрементируется при каждой смене лидера
- **counter** — инкрементируется для каждой транзакции внутри эпохи; сбрасывается при новом epoch

## Отклонения от оригинального алгоритма

| Аспект | Оригинал (ZooKeeper) | Симуляция |
|--------|---------------------|-----------|
| Fast Leader Election | Обмен notification-ами с retry | Broadcast голосов, кворумное решение |
| Discovery phase | Отдельная фаза discovery | Совмещена с election |
| Transaction log | WAL на диске с snapshot | Только в памяти |
| Quorum | Configurable (возможны weighted quorums) | Простое большинство |
| Learner (Observer) | Не голосующие read-only узлы | Не реализованы |
| FIFO guarantees | TCP гарантирует FIFO между парами | Симуляция не моделирует FIFO per-pair |
| Commit mapping | Commit привязан к конкретному `zxid` и transaction log | Учебная модель с explicit `zxid`, sync и commit по идентичности записи |
| PRNG | Не специфицирован | Внутренние таймауты выборов используют seeded RNG |

## Источники

1. **Junqueira F., Reed B., Serafini M.** "Zab: High-performance broadcast for primary-backup systems" (2011) — [IEEE DSN](https://doi.org/10.1109/DSN.2011.5958223)
2. **Reed B., Junqueira F.** "A simple totally ordered broadcast protocol" (2008) — [ACM LADIS](https://www.datadoghq.com/pdf/zab.totally-ordered-broadcast-protocol.2008.pdf)
3. **Hunt P., Konar M., Junqueira F., Reed B.** "ZooKeeper: Wait-free Coordination for Internet-scale Systems" (2010) — [USENIX ATC](https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf)

::: tip Попробуйте в симуляторе
Откройте [симулятор](https://khorost.github.io/consensus-landscape/), поставьте рядом Zab и Raft. Отключите лидера и сравните: в Zab после выборов видна явная фаза синхронизации (Sync → NewLeader → AckNewLeader) перед началом обработки запросов, а в Raft лидер начинает работать сразу.
:::
