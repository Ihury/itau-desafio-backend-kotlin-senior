# Contrato de observabilidade

Os nomes abaixo são os nomes Micrometer; no Prometheus, pontos viram `_` e contadores ganham `_total`.

## Métricas de negócio (Micrometer -> `/actuator/prometheus`, porta de gerenciamento)

| Métrica | Tipo | Tags | Semântica |
|---------|------|------|-----------|
| `balance.events` | counter | `outcome` = `processed`\|`obsolete`\|`duplicate`\|`rejected`; `reason` = `none` ou código de `kafka-events.md` (só em `rejected`) | **Exatamente um** desfecho por mensagem consumida (FR-031/SC-010). `rejected` é contado quando o DLT confirma a publicação (`RetryListener.recovered`), nunca em tentativas. Alertar em `reason=unprocessable_event` (defeito interno: mensagem válida isolada) |
| `balance.events.anomalies` | counter | `type` = `conflicting_duplicate` | Mesmo `transaction.id` e mesmo timestamp com conteúdo divergente (defeito da origem); detectado comparando o item retornado por `ALL_OLD`. Desfecho continua `duplicate` |
| `balance.ingest.duration` | timer (histograma, SLO 5 ms..2,5 s) | `outcome` = `processed`\|`obsolete`\|`duplicate`\|`rejected`\|`error` | Parse + validação + escrita, **por entrega** (não por mensagem): uma mensagem reentregue por falha transitória gera uma amostra por tentativa, com `outcome=error` nas que falham |
| `balance.store.write.duration` | timer (histograma, SLO 5 ms..2 s) | `result` = `applied`\|`condition_failed`\|`error` | Latência da `UpdateItem` condicional (só a chamada; a classificação posterior de `condition_failed` não entra) |
| `balance.store.read.duration` | timer (histograma, SLO 5 ms..2 s) | `result` = `found`\|`not_found`\|`error` | Latência do `GetItem`, inclusive quando o SDK lança; item corrompido conta como `found` (o banco respondeu) |
| `balance.store.read.corrupted` | counter | — | Item do snapshot ilegível ou fora do layout/limites do domínio, encontrado na leitura: falha interna, a API responde 500 (nunca 404, 400 nem dado errado) e o item é logado sem valores. Alertar em qualquer incremento |
| `balance.consumer.backpressure` | counter | `cause` = `throttled`\|`unavailable`\|`timeout`\|`misconfigured` | Pausas do container por falha transitória (medidor de indisponibilidade da ingestão); `misconfigured` = tabela inexistente, acesso negado ou credencial ausente/inválida/expirada (continua transitória, nunca DLT) |
| `balance.dlt.publish.failures` | counter | — | Falhas ao publicar no DLT (mensagem não confirmada; alertar) |
| `http.server.requests` | timer (Boot) | `uri`, `status`, `outcome` | Latência da API; histograma habilitado com SLO (50 ms, 100 ms, 300 ms, 1 s, 2 s) |
| `balance.dependency.up` | gauge | `dependency=dynamodb` | 1 = último probe OK, 0 = falha (mesmo estado do grupo `dependencies`); base do alerta de indisponibilidade sem tirar a instância de rotação |
| `resilience4j.circuitbreaker.*` | gauges/counters | `name=dynamodb-read` | Estado e taxas do circuit breaker (`resilience4j-micrometer`) |
| `kafka.consumer.*`, `spring.kafka.listener` | Boot | — | Lag e tempos do consumer (`spring.kafka.listener.observation-enabled=true`) |

Consultas de referência (SC-011):

```promql
# proporção de mensagens rejeitadas / obsoletas na última janela
sum(rate(balance_events_total{outcome="rejected"}[5m])) / sum(rate(balance_events_total[5m]))
sum(rate(balance_events_total{outcome="obsolete"}[5m])) / sum(rate(balance_events_total[5m]))
# p50 / p99 da API e da escrita
histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{uri="/balances/{accountId}"}[5m])))
histogram_quantile(0.99, sum by (le) (rate(balance_store_write_duration_seconds_bucket[5m])))
```

## Health checks (Actuator, `MANAGEMENT_SERVER_PORT`)

| Endpoint | Conteúdo | Comportamento |
|----------|----------|---------------|
| `/actuator/health/liveness` | `livenessState` apenas | Permanece `UP` com o DynamoDB fora (FR-033) |
| `/actuator/health/readiness` | `readinessState` **apenas** (capacidade do próprio processo de atender) | Permanece `UP` com o DynamoDB fora: a instância **não sai de rotação**, e a API responde 503 + `Retry-After` de forma explícita (circuit breaker, SC-008). Sem dependência compartilhada na readiness |
| `/actuator/health/dependencies` | `DynamoDbHealthIndicator`: probe `DescribeTable` da tabela, cache de 5 s, timeout curto; `show-details=never` | `DOWN` (503) quando o DynamoDB falha; **não** pertence a `liveness`/`readiness`; observável por operação/alerta (FR-033) |
| `/actuator/health` (raiz) | agrega o grupo `dependencies` | **Não usar** em balanceador/orquestrador: fica 503 com o DynamoDB fora; usar `liveness`/`readiness` |
| `/actuator/prometheus`, `/actuator/info` | métricas / build info | Somente na porta de gerenciamento (`MANAGEMENT_SERVER_PORT`, 8082), nunca na porta da API (8080). No compose local a 8082 é publicada no host (`8082:8082`); em produção não deve ser roteada pelo balanceador público |

## Logs (JSON nativo do Spring Boot: `logging.structured.format.console=logstash`)

- Campos MDC como chaves de topo: `correlationId` (HTTP: `X-Correlation-Id` validado ou gerado; Kafka:
  `<topic>-<partition>@<offset>`), `accountId`, `transactionId` (somente quando o evento foi parseado com sucesso).
- **Nunca** logados: saldo/valores, `owner`, payload, mensagens de exceção de parsers. Falha de validação loga apenas
  motivo, tópico/partição/offset.
- Nenhum `catch` engole exceção sem log + métrica correspondentes (FR-035).
- Tracing distribuído (OpenTelemetry) não é implementado: ver `research.md` R-16.
