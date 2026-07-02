# Другие алгоритмы консенсуса

Ниже перечислены известные алгоритмы консенсуса, которые **не реализованы** в текущей версии симулятора, но представляют значительный интерес для изучения. Реализованные алгоритмы: [Raft](./algorithms/raft), [Basic Paxos](./algorithms/paxos), [Multi-Paxos](./algorithms/multi-paxos), [Zab](./algorithms/zab), [EPaxos](./algorithms/epaxos).

## Viewstamped Replication (VR)

**Год:** 1988 (оригинал), 2012 (revisited)

Алгоритм с лидером, предшественник Raft. Использует концепцию `view` (аналог `term` в Raft) и протокол смены `view` при отказе лидера. Исторически значим как один из первых алгоритмов реплицированного автомата.

VR и Raft структурно очень похожи: оба используют выделенного лидера, репликацию лога и кворумные подтверждения. Основное различие — в механизме `view change` против обычных выборов.

**Источники:**
- Oki B., Liskov B. "Viewstamped Replication: A New Primary Copy Method to Support Highly-Available Distributed Systems" (1988) — [ACM PODC](https://doi.org/10.1145/62546.62549)
- Liskov B., Cowling J. "Viewstamped Replication Revisited" (2012) — [MIT CSAIL](https://dspace.mit.edu/handle/1721.1/71763)

---

## PBFT (Practical Byzantine Fault Tolerance)

**Год:** 1999

Первый практически применимый алгоритм, устойчивый к **византийским** отказам — когда узлы могут вести себя произвольно (врать, отправлять противоречивые сообщения). Требует `3f + 1` узлов для толерантности к `f` византийским отказам.

Трёхфазный протокол: Pre-prepare → Prepare → Commit. Значительно дороже crash-fault алгоритмов (Raft, Paxos) по количеству сообщений: O(n²) на операцию.

**Источники:**
- Castro M., Liskov B. "Practical Byzantine Fault Tolerance" (1999) — [OSDI](https://doi.org/10.5555/296806.296824)
- Castro M., Liskov B. "Practical Byzantine Fault Tolerance and Proactive Recovery" (2002) — [ACM TOCS](https://doi.org/10.1145/571637.571640)

---

## Tendermint / CometBFT

**Год:** 2014 (Tendermint), переименован в CometBFT в 2023

BFT-алгоритм консенсуса, разработанный для блокчейн-систем. Раундовый протокол: Propose → Prevote → Precommit. Толерантен к `f < n/3` византийским узлам.

Широко используется в экосистеме Cosmos (межблокчейн-коммуникация). Отличается от PBFT более простой структурой и раундовым подходом вместо `view change`.

**Источники:**
- Buchman E. "Tendermint: Byzantine Fault Tolerance in the Age of Blockchains" (2016) — [MSc Thesis](https://knowen-production.s3.amazonaws.com/uploads/attachment/file/1814/Buchman_Ethan_201606_MAsc.pdf)
- Buchman E., Kwon J., Milosevic Z. "The latest gossip on BFT consensus" (2018) — [arXiv:1807.04938](https://arxiv.org/abs/1807.04938)

---

## HotStuff

**Год:** 2019

BFT-алгоритм с **линейной** сложностью по сообщениям (O(n) вместо O(n²) у PBFT). Достигается за счёт pipeline-архитектуры: каждая фаза следующего раунда подтверждает предыдущий. Использован в проекте Meta Diem (бывший Libra).

Три фазы: Prepare → Pre-commit → Commit, но благодаря chaining каждая фаза одновременно обслуживает несколько раундов.

**Источники:**
- Yin M., Malkhi D., Reiter M.K., Gueta G.G., Abraham I. "HotStuff: BFT Consensus with Linearity and Responsiveness" (2019) — [ACM PODC](https://arxiv.org/abs/1803.05069)

---

## Сравнительная таблица

| Алгоритм | Год | Fault model | Лидер | Сообщений на коммит | Узлов для `f` отказов |
|----------|-----|-------------|-------|--------------------|-----------------------|
| **Paxos** ✅ | 1989 | Crash | Нет | O(n) | 2f + 1 |
| **Multi-Paxos** ✅ | 1989 | Crash | Да | O(n) | 2f + 1 |
| **VR** | 1988 | Crash | Да | O(n) | 2f + 1 |
| **Raft** ✅ | 2014 | Crash | Да | O(n) | 2f + 1 |
| **Zab** ✅ | 2011 | Crash | Да | O(n) | 2f + 1 |
| **EPaxos** ✅ | 2013 | Crash | Нет | O(n) (`fast path`) | 2f + 1 |
| **PBFT** | 1999 | Byzantine | Да (rotating) | O(n²) | 3f + 1 |
| **Tendermint** | 2014 | Byzantine | Да (rotating) | O(n²) | 3f + 1 |
| **HotStuff** | 2019 | Byzantine | Да (rotating) | O(n) | 3f + 1 |

✅ — реализован в симуляторе
