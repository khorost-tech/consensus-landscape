# Basic Paxos

![Paxos — двухфазный консенсус](/img/paxos-consensus.png)

## Обзор

Paxos — алгоритм консенсуса, придуманный Leslie Lamport в 1989 году и опубликованный в 1998 году в статье "The Part-Time Parliament". Это исторически **первый корректно доказанный** алгоритм распределённого консенсуса.

**Ключевые особенности:**
- Нет выделенного лидера — любой узел может предложить значение
- Двухфазный протокол: Prepare/Promise → Accept/Accepted
- Гарантирует согласие при наличии кворума, но не гарантирует прогресс (livelock при «дуэли» proposer-ов)

## Роли узлов

В Classic Paxos каждый узел совмещает роли **proposer** и **acceptor**:

| Роль | Цвет в симуляторе | Поведение |
|------|-------------------|-----------|
| **Acceptor** | 🔵 синий | Отвечает на Prepare/Accept; пассивное состояние |
| **Proposer** | 🟡 жёлтый | Активно предлагает значение; проводит раунд консенсуса |

Узел становится proposer при получении клиентского запроса и возвращается в acceptor после коммита.

## Нумерация предложений

Для глобальной уникальности номера предложений генерируются по формуле:

```
proposalNumber = seqNum × nodeCount + nodeIndex
```

Например, при 3 узлах: node_0 использует номера 3, 6, 9…; node_1 — 4, 7, 10…; node_2 — 5, 8, 11…

## Фаза 1: Prepare / Promise

Proposer рассылает `Prepare(n)` всем acceptor-ам. Каждый acceptor:
- Если `n > minProposal` → обещает не принимать предложения с номером ниже `n`, отвечает `Promise` с ранее принятым значением (если есть)
- Если `n ≤ minProposal` → отвечает `NACK`

```mermaid
sequenceDiagram
    participant P as Proposer (Node 0)
    participant A1 as Acceptor (Node 1)
    participant A2 as Acceptor (Node 2)

    Note over P: Клиент: "set x=1"
    P->>P: Prepare(n=3) — self-promise
    P->>A1: Prepare(n=3)
    P->>A2: Prepare(n=3)
    A1->>P: Promise(n=3, accepted=∅)
    Note over P: 2/3 promises → кворум!
    A2->>P: Promise(n=3, accepted=∅)
```

## Фаза 2: Accept / Accepted

Получив кворум Promise, proposer рассылает `Accept(n, value)`:
- Если какой-то acceptor сообщил ранее принятое значение — proposer **обязан** использовать его (правило Paxos)
- Иначе предлагает своё значение

```mermaid
sequenceDiagram
    participant P as Proposer (Node 0)
    participant A1 as Acceptor (Node 1)
    participant A2 as Acceptor (Node 2)

    P->>A1: Accept(n=3, "set x=1")
    P->>A2: Accept(n=3, "set x=1")
    A1->>P: Accepted(n=3)
    Note over P: 2/3 accepted → committed!
    P->>A1: Learn("set x=1")
    P->>A2: Learn("set x=1")
```

## Фаза Learn

После получения кворума Accepted, proposer:
1. Добавляет значение в свой лог как committed
2. Рассылает `Learn` всем узлам, чтобы они обновили свои логи

## Dueling Proposers

Если два узла одновременно начинают Prepare с разными номерами, они могут блокировать друг друга — каждый NACK-ает другого:

```mermaid
sequenceDiagram
    participant P1 as Proposer 1
    participant A as Acceptor
    participant P2 as Proposer 2

    P1->>A: Prepare(n=3)
    A->>P1: Promise(n=3)
    P2->>A: Prepare(n=4)
    A->>P2: Promise(n=4)
    P1->>A: Accept(n=3, v1)
    A->>P1: NACK (promised n=4)
    P1->>A: Prepare(n=6)
    A->>P1: Promise(n=6)
    P2->>A: Accept(n=4, v2)
    A->>P2: NACK (promised n=6)
    Note over P1,P2: Бесконечный цикл без backoff!
```

### Решение в симуляции: неравномерный backoff

Чтобы разорвать симметрию, таймаут отступа зависит от индекса узла:

```
backoff = BASE + nodeIndex × PER_NODE + random(0, JITTER)
```

Узлы с меньшим индексом возвращаются раньше, что даёт им преимущество. Значения по умолчанию:
- `BASE = 200 мс`, `PER_NODE = 120 мс`, `JITTER = 100 мс`

Аналогичная формула используется для таймаута в Prepare-фазе:
- `BASE = 200 мс`, `PER_NODE = 80 мс`, `JITTER = 150 мс`

## Восстановление узла

При восстановлении отключённого узла движок находит лучшего живого peer-а и отправляет `Learn` для всех пропущенных committed записей. Это реализует **state transfer** — упрощённый аналог snapshot/catch-up.

## Полный цикл коммита

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Proposer
    participant A1 as Acceptor 1
    participant A2 as Acceptor 2

    C->>P: "set x=1"
    P->>A1: Prepare(n=3)
    P->>A2: Prepare(n=3)
    A1->>P: Promise ✓
    Note over P: Кворум promises
    P->>A1: Accept(n=3, "set x=1")
    P->>A2: Accept(n=3, "set x=1")
    A1->>P: Accepted ✓
    Note over P: Кворум accepted → COMMIT
    P->>A1: Learn("set x=1")
    P->>A2: Learn("set x=1")
    P-->>C: Committed
```

## Отклонения от оригинального алгоритма

| Аспект | Оригинал (Lamport) | Симуляция |
|--------|-------------------|-----------|
| Модель | Single-decree Paxos (одно значение) | Slot-based учебная модель: отдельный экземпляр Paxos на каждый слот |
| Multi-Paxos | Стабильный лидер после первого Prepare | Не реализован; каждая команда — полный 2-фазный цикл |
| Learner | Выделенная роль learner | Нет; proposer рассылает Learn всем |
| Персистентность | `minProposal`, `acceptedProposal`, `acceptedValue` на диске | Только в памяти |
| Порядок в логе | Определяется номером слота | Сохраняется на уровне slot/index, но без production-level state transfer semantics |
| Acceptor state | Хранится между раундами | Хранится отдельно для каждого слота, persistent storage не моделируется |
| PRNG | Не специфицирован | Сетевые задержки, таймауты и backoff идут через seeded RNG |

## Источники

1. **Lamport L.** "The Part-Time Parliament" (1998) — [ACM TOCS](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf)
2. **Lamport L.** "Paxos Made Simple" (2001) — [Microsoft Research](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)
3. **Van Renesse R., Altinbuken D.** "Paxos Made Moderately Complex" (2015) — [ACM Computing Surveys](https://doi.org/10.1145/2673577)

::: tip Попробуйте в симуляторе
Откройте [симулятор](https://khorost.github.io/consensus-landscape/), выберите Paxos, добавьте вторую панель с Raft и сравните количество сообщений для одной операции записи.
:::
