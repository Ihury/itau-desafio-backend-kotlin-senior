# ADR-0008: Erros transitórios e permanentes, backpressure e DLT

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípios III e V; `specs/001-consulta-saldo/research.md` R-08; FR-016, FR-017, FR-028, FR-029; SC-007

## Contexto

O consumer nunca pode perder uma mensagem válida (Constitution III) e nunca pode isolar no DLT uma mensagem válida por causa de uma
falha de infraestrutura (FR-017): com o DynamoDB fora, a mensagem deve ficar no broker e ser reprocessada quando ele voltar. Ao mesmo
tempo, uma mensagem que nunca vai funcionar não pode bloquear a partição para sempre. O `DefaultErrorHandler` padrão do Spring Kafka
(`FixedBackOff(0, 9)` seguido de log e *skip*) descarta a mensagem depois de ~10 falhas e por isso não serve.

## Decisão

Um único `CommonErrorHandler` (`DeadLetterConfig.kafkaErrorHandler`, `DefaultErrorHandler` com `DeadLetterPublishingRecoverer`) que
classifica cada falha **por classe da exceção** (`FailureClassifier`), nunca por mensagem:

| Classe | Exceção | Tratamento |
|--------|---------|-----------|
| **Permanente** | `InvalidEventException` | Não retentável: DLT imediato, com os bytes originais e `x-rejection-reason`/`-detail`/`-at`; desfecho `rejected{reason}` |
| **Transitória** | `BalanceStoreUnavailableException(failureCause)` | `ExponentialBackOff` 500 ms x2, máximo 30 s, jitter 250 ms, **sem limite de tentativas nem de tempo**, com o container **pausado** durante a espera; **nunca** DLT |
| **Não classificada** | qualquer outra (inclui `BalanceStoreRejectedException`) | `FixedBackOff(100 ms, 2)` (3 entregas) e DLT `unprocessable_event`: um defeito determinístico não bloqueia a partição para sempre |

**Backpressure da transitória.** Durante a espera do backoff o `ContainerPausingBackOffHandler` pausa o container (o filho que falhou,
um por thread de consumo) e agenda a retomada num `TaskScheduler` próprio (`BackpressureConfig`). O poll continua vivo, então esperas
maiores que `max.poll.interval.ms` não provocam rebalance; o registro falho é reposicionado (`seek`) e reentregue na retomada, e a
mensagem nunca é confirmada até ser persistida. Cada entrega que falha conta `balance.consumer.backpressure{cause}`
(`throttled|unavailable|timeout`, a `StoreFailureCause` da exceção). O jitter é o nativo do `ExponentialBackOff` do Spring Framework 7
(`setJitter`): o intervalo varia entre `intervalo - jitter` e `intervalo + jitter`, com o jitter escalado pelo multiplicador, e nunca
passa do máximo. Os parâmetros vêm de `KAFKA_BACKOFF_INITIAL_MS`, `KAFKA_BACKOFF_MAX_MS` e `KAFKA_BACKOFF_JITTER_MS`
(`contracts/configuration.md`).

**Uma única camada de retry.** O cliente de escrita do SDK não tenta de novo (ADR-0009); o único retry da escrita é o do error handler.
O backoff padrão do `DefaultErrorHandler` é o da transitória (nunca um que se esgote): com um padrão que esgota, o Spring ignoraria a
função de backoff por classe e recuperaria toda falha na primeira entrega.

**DLT indisponível = não confirma.** A publicação no DLT é síncrona (`waitForSendResultTimeout=5 s`, `max.block.ms=3 s`, `acks=all`,
idempotência). Se falhar, o recoverer lança, o offset não é confirmado, o registro é reentregue, o container segue vivo e
`balance.dlt.publish.failures` cresce. O desfecho `rejected` só é contado depois que o DLT confirma.

## Limite conhecido (risco R2)

Enquanto o DLT está fora, a nova tentativa de publicar não tem backoff exponencial próprio: cada tentativa fica limitada por
`max.block.ms` + `waitForSendResultTimeout` (~3-5 s). A mensagem nunca se perde; o sinal operacional é `balance.dlt.publish.failures`.
A pausa do container também é por thread de consumo: as demais partições da mesma instância seguem processando (e falhando, se o
DynamoDB estiver fora para todas).

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| `@RetryableTopic` (tópicos de retry) | Mais tópicos e reordenação; o broker já é o buffer da mensagem retida |
| Retry em memória bloqueante (`Thread.sleep` no thread do poll) | Estoura `max.poll.interval.ms` (rebalance) e desperdiça a thread; medido: sem a pausa o container não fica pausado |
| Pausa manual com `Consumer.pause` | Reimplementaria o `ContainerPausingBackOffHandler` nativo |
| Backoff que esgota e vai ao DLT | Isolaria mensagem válida por falha de infraestrutura (viola FR-017) |
| Retry também no SDK de escrita | Segunda camada de retry: proibida pela Constitution V (ADR-0009) |

## Consequências

- (+) Indisponibilidade do armazenamento nunca perde nem isola mensagem válida; a retomada é automática (SC-007).
- (+) Sem rebalance durante esperas longas, pois o poll continua vivo com o container pausado.
- (+) Parâmetros do backoff por variável de ambiente; o jitter evita que várias instâncias retomem juntas.
- (-) Depois que o backoff atinge o teto, a retomada pode levar até 30 s após o armazenamento voltar.
- (-) Uma falha permanente do armazenamento (p.ex. credencial) é tratada como transitória e retém a partição indefinidamente: o sinal é
  `balance.consumer.backpressure` crescendo continuamente.
