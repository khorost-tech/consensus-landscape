# Multi-Paxos

## Обзор

Multi-Paxos — оптимизация Basic Paxos, предложенная Leslie Lamport. Ключевая идея: после выбора лидера через Prepare/Promise (фаза 1) все последующие команды могут пропускать фазу 1 и сразу отправляться через Accept/Accepted. Это сокращает латентность в устойчивом режиме с 2 RTT до 1 RTT.

**Ключевые особенности:**
- Стабильный лидер — после выборов обрабатывает команды без повторного Prepare
- Первая команда после (пере)выборов идёт полным путём (2 RTT), остальные — быстрым (1 RTT)
- `heartbeat`-сообщения для поддержания лидерства
- При потере лидера — полный цикл Paxos заново

## Роли узлов

```mermaid
stateDiagram-v2
    [*] --> Follower
    Follower --> Candidate : Election timeout
    Candidate --> Leader : Кворум promises
    Candidate --> Follower : NACK / больший ballot
    Leader --> Follower : NACK / больший ballot
```

| Роль | Цвет в симуляторе | Поведение |
|------|-------------------|-----------|
| **Follower** | 🔵 синий | Принимает Accept, отвечает Accepted; сбрасывает election timer по heartbeat |
| **Candidate** | 🟡 жёлтый | Отправляет Prepare, собирает Promise |
| **Leader** | 🟢 зелёный | Отправляет Accept напрямую (пропуская Prepare); рассылает heartbeats |

## Нумерация Ballot

Как и в Basic Paxos, номера `ballot` глобально уникальны:

```
ballotNumber = seqNum × nodeCount + nodeIndex
```

## Выборы лидера (Phase 1)

Выборы используют стандартный механизм Paxos: Prepare → Promise.

```mermaid
sequenceDiagram
    participant N1 as Node 0 (Follower)
    participant N2 as Node 1 (Follower)
    participant N3 as Node 2 (Follower)

    Note over N1: Election timeout!
    N1->>N1: ballot++ → Candidate
    N1->>N2: Prepare(ballot=3)
    N1->>N3: Prepare(ballot=3)
    N2->>N1: Promise(ballot=3)
    Note over N1: 2/3 promises → Leader
    N1->>N2: Heartbeat
    N1->>N3: Heartbeat
```

## Первая команда: полный Paxos (2 RTT)

После избрания лидер ещё не «установлен» — первая команда проходит через обе фазы для подтверждения лидерства:

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2

    C->>L: "set x=1"
    Note over L: Первая команда — полный Paxos
    L->>F1: Prepare(ballot)
    L->>F2: Prepare(ballot)
    F1->>L: Promise ✓
    Note over L: Кворум promises → Phase 2
    L->>F1: Accept(ballot, "set x=1")
    L->>F2: Accept(ballot, "set x=1")
    F1->>L: Accepted ✓
    Note over L: Кворум accepted → COMMIT
    L->>F1: Learn("set x=1")
    L->>F2: Learn("set x=1")
    L-->>C: Committed
    Note over L: leaderEstablished = true
```

## Последующие команды: оптимизированный путь (1 RTT)

Лидер пропускает Phase 1 и отправляет Accept напрямую:

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2

    C->>L: "set y=2"
    Note over L: Phase 1 пропущена!
    L->>F1: Accept(ballot, "set y=2")
    L->>F2: Accept(ballot, "set y=2")
    F1->>L: Accepted ✓
    Note over L: Кворум → COMMIT
    L->>F1: Learn("set y=2")
    L->>F2: Learn("set y=2")
    L-->>C: Committed
```

## Heartbeats

Лидер периодически рассылает `heartbeat`-сообщения для:
1. Подтверждения лидерства (предотвращает ненужные выборы)
2. Сброса election timer у follower-ов

| Параметр | LAN | WAN | Global |
|----------|-----|-----|--------|
| Election timeout | 50–100 мс | 300–600 мс | 1000–2000 мс |
| Heartbeat interval | 20 мс | 100 мс | 300 мс |

## Обработка отказов

### Потеря лидера

1. Heartbeats прекращаются → election timer срабатывает у follower-а
2. Follower становится candidate → отправляет Prepare
3. Получает кворум Promise → становится новым лидером
4. Первая команда снова идёт полным путём (2 RTT)
5. После первого коммита — переход на быстрый путь (1 RTT)

### NACK и уступка лидерства

Если acceptor получает Accept или Prepare с ballot ниже своего `minProposal`, он отвечает NACK. Лидер, получивший NACK, уступает лидерство:
- Становится follower
- Пересчитывает seqNum, чтобы следующий ballot был выше
- Ждёт backoff перед повторной попыткой

## Отклонения от оригинального алгоритма

| Аспект | Оригинал (Lamport) | Симуляция |
|--------|-------------------|-----------|
| Multi-decree | Leader устанавливается одним Prepare для всех слотов | Первая команда повторяет Prepare для наглядности |
| Слоты | Каждое значение привязано к номеру слота | Учебная slot-based модель с отдельным состоянием acceptor на слот |
| Персистентность | ballot, accepted value на диске | Только в памяти |
| Pipelining | Несколько Accept параллельно | Одна команда за раз |
| Learner | Выделенная роль | Нет; лидер рассылает Learn |
| PRNG | Не специфицирован | Внутренние таймауты и backoff используют seeded RNG |

## Источники

1. **Lamport L.** "The Part-Time Parliament" (1998) — [ACM TOCS](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf)
2. **Lamport L.** "Paxos Made Simple" (2001) — [Microsoft Research](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)
3. **Van Renesse R., Altinbuken D.** "Paxos Made Moderately Complex" (2015) — [ACM Computing Surveys](https://doi.org/10.1145/2673577)

::: tip Попробуйте в симуляторе
Откройте [симулятор](https://khorost.github.io/consensus-landscape/), поставьте рядом Multi-Paxos и Basic Paxos. Обратите внимание: после выборов первая команда в Multi-Paxos идёт через Prepare→Accept (2 RTT), а вторая — только через Accept (1 RTT). В Basic Paxos каждая команда всегда проходит оба этапа.
:::
