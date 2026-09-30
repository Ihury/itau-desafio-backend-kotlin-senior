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

**Backpressure da transitória.** Durante a espera do backoff o `ContainerPausingBackOffHandler` pausa o container e agenda a retomada
num `TaskScheduler` próprio (`BackpressureConfig`). **O handler recebe o container PAI** (`thisOrParentContainer` do Spring Kafka 4.1,
verificado no bytecode), então a pausa vale para **todas as threads de consumo da instância**, não só para a que falhou. O poll
continua vivo, então esperas maiores que `max.poll.interval.ms` não provocam rebalance; o registro falho é reposicionado (`seek`) e reentregue na retomada, e a
mensagem nunca é confirmada até ser persistida. Cada entrega que falha conta `balance.consumer.backpressure{cause}`
(`throttled|unavailable|timeout|misconfigured`, a `StoreFailureCause` da exceção; `misconfigured` cobre tabela inexistente, acesso negado
e credencial ausente, inválida ou expirada: só muda o diagnóstico, o tratamento continua transitório e a falha é logada em ERROR). O jitter é o nativo do `ExponentialBackOff` do Spring Framework 7
(`setJitter`): o intervalo varia entre `intervalo - jitter` e `intervalo + jitter`, com o jitter escalado pelo multiplicador, e nunca
passa do máximo. Os parâmetros vêm de `KAFKA_BACKOFF_INITIAL_MS`, `KAFKA_BACKOFF_MAX_MS` e `KAFKA_BACKOFF_JITTER_MS`
(`contracts/configuration.md`).

**Uma única camada de retry.** O cliente de escrita do SDK não tenta de novo (ADR-0009); o único retry da escrita é o do error handler.
O backoff padrão do `DefaultErrorHandler` é o da transitória (nunca um que se esgote): com um padrão que esgota, o Spring ignoraria a
função de backoff por classe e recuperaria toda falha na primeira entrega.

**DLT indisponível = não confirma.** A publicação no DLT é síncrona (`waitForSendResultTimeout=5 s`, `max.block.ms=3 s`, `acks=all`,
idempotência). Se falhar, o recoverer lança, o offset não é confirmado, o registro é reentregue, o container segue vivo e
`balance.dlt.publish.failures` cresce. O desfecho `rejected` só é contado depois que o DLT confirma.

## Limites conhecidos

**Escopo da pausa (por instância).** A pausa atinge todas as threads da instância. É o comportamento certo para a indisponibilidade
geral do DynamoDB, porque todas as threads falhariam de qualquer modo, e evita rajadas de tentativas inúteis. É excessivo para
*throttling* de uma conta quente: as partições que não têm culpa ficam paradas até a retomada. Evolução: pausa por thread ou por
partição, por exemplo pausando só o container filho (por id, com o registro de containers no `ListenerContainerPauseService`) ou só as
partições da thread que falhou. Não foi feito porque não há requisito nem medição que mostre conta quente, e a pausa geral é a mais simples de
provar correta.

**Risco R2: DLT fora.** Enquanto o DLT está fora, a nova tentativa de publicar não tem backoff exponencial próprio: cada tentativa
fica limitada por `max.block.ms` + `waitForSendResultTimeout` (~3-5 s) e o laço de reentrega **ocupa a thread de consumo inteira**,
não só a partição da mensagem: as outras partições atribuídas à mesma thread param de avançar até o DLT voltar (as demais threads da
instância seguem). A mensagem nunca se perde; o sinal operacional é `balance.dlt.publish.failures`. Evolução: `BackOff` dedicado à
falha de publicação no DLT, com pausa do container.

**DLT é at-least-once.** Se o processo cair depois que o DLT confirma e antes de o offset de entrada ser confirmado, a mensagem é
reentregue e publicada de novo: o DLT pode ter duplicata e `rejected` pode contar em dobro. Nunca há perda.

**Defeito sistêmico.** Uma regressão que faça mensagens válidas falharem como "não classificada" as isola no DLT como
`unprocessable_event` (3 entregas cada), sem perder nenhuma, mas exigindo replay. Hoje: alerta em
`rate(balance_events_total{reason="unprocessable_event"}[5m]) > 0` e roteiro de reprocessamento manual (`kafka-events.md` seção 7).
Evolução: um "fusível" que, acima de uma taxa de `unprocessable_event`, trate a falha como transitória (pausa, mensagem no broker).

**Limites de tamanho e configuração.** `max.request.size` do produtor do DLT e `max.message.bytes` do DLT não são alinhados
explicitamente ao tópico de entrada (valem os defaults), e o invariante `max.poll.records x write call timeout < max.poll.interval.ms`
e `dlt != topic` não são validados na partida (o invariante é verificado por teste com os valores padrão). Evolução: alinhar os
limites e validar a configuração na partida.

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| `@RetryableTopic` (tópicos de retry) | Mais tópicos e reordenação; o broker já é o buffer da mensagem retida |
| Retry em memória bloqueante (`Thread.sleep` no thread do poll) | Estoura `max.poll.interval.ms` (rebalance) e desperdiça a thread; medido: sem a pausa o container não fica pausado |
| Pausa manual com `Consumer.pause` | Reimplementaria o `ContainerPausingBackOffHandler` nativo |
| Pausa por thread ou por partição | Exigiria o registro de containers ou controle de partições; a pausa do container pai é a mais simples e correta para a indisponibilidade geral. Fica como evolução (acima) |
| Backoff que esgota e vai ao DLT | Isolaria mensagem válida por falha de infraestrutura (viola FR-017) |
| Retry também no SDK de escrita | Segunda camada de retry: proibida pela Constitution V (ADR-0009) |

## Consequências

- (+) Indisponibilidade do armazenamento nunca perde nem isola mensagem válida; a retomada é automática (SC-007).
- (+) Sem rebalance durante esperas longas, pois o poll continua vivo com o container pausado.
- (+) Parâmetros do backoff por variável de ambiente; o jitter evita que várias instâncias retomem juntas.
- (-) Depois que o backoff atinge o teto, a retomada pode levar até 30 s após o armazenamento voltar.
- (-) Uma falha permanente do armazenamento (p.ex. credencial) é tratada como transitória e retém a ingestão indefinidamente: o sinal é
  `balance.consumer.backpressure` crescendo continuamente, com `cause=misconfigured` quando a causa é configuração ou credencial.
- (-) A pausa por instância também para partições que não falharam (ver "Limites conhecidos").
