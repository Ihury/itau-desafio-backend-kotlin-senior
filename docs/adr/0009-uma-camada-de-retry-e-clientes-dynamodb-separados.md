# ADR-0009: Uma camada de retry e clientes DynamoDB separados

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípio V; `specs/001-consulta-saldo/research.md` R-08 e R-10; SC-008

## Contexto

O cliente DynamoDB padrão do AWS SDK usa até 9 tentativas com backoff próprio. Somar isso ao backoff do consumer Kafka empilha
retentativas e gera tempestade de requisições contra um armazenamento que já está degradado. A Constitution V exige **exatamente
uma camada de retry por chamada remota**, com timeouts explícitos em toda chamada.

A escrita e a leitura têm necessidades opostas:

- **Escrita (consumer)**: indisponibilidades longas exigem redelivery e pausa do container de qualquer forma, logo o error handler
  do consumer já é a camada de retry (ADR-0008); o SDK não pode ser uma segunda.
- **Leitura (API)**: não há redelivery; a única camada possível é o SDK, curta, para falhar em até 2 s (SC-008).

## Decisão

Dois `DynamoDbClient`, com política de retry, timeouts e pool próprios (`DynamoDbClientsConfig`, `@Qualifier`):

| | Cliente de **escrita** (`dynamoDbWriteClient`) | Cliente de **leitura** (`dynamoDbReadClient`) |
|---|---|---|
| Camada de retry | Error handler do consumer. SDK: `AwsRetryStrategy.doNotRetry()` (1 tentativa) | SDK `standard`, `maxAttempts=2` (1 retry) |
| `apiCallAttemptTimeout` / `apiCallTimeout` | 2 s / 2 s (`DYNAMODB_WRITE_*`) | 0,6 s / 1,5 s (`DYNAMODB_READ_*`) |
| Conexão / aquisição do pool | 0,3 s / 0,3 s | 0,3 s / 0,3 s |
| Pool (`maxConnections`) | 50 | 100 (falha rápida ao esgotar = bulkhead) |
| Circuit breaker | Não: o backpressure do consumer cumpre o papel | Sim (ADR-0010) |

Invariante do consumer: `max.poll.records x DYNAMODB_WRITE_CALL_TIMEOUT < max.poll.interval.ms` (100 x 2 s = 200 s < 300 s),
verificada por teste, para que o pior caso de um lote não estoure o intervalo de poll.

## Por que dois clientes

`RequestOverrideConfiguration` permite sobrescrever timeouts por requisição, mas **não** a estratégia de retry (verificado no
`sdk-core` 2.46.7). Um cliente único forçaria retry duplicado ou timeouts iguais para necessidades diferentes. Dois clientes também
**isolam os pools**: uma rajada de ingestão não esgota as conexões da API de consulta.

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| SDK com 3 tentativas + backoff do consumer | Duas camadas de retry: proibido pela Constitution V |
| Um cliente único com timeouts por requisição | Não isola o pool nem permite retry distinto entre leitura e escrita |
| `@Retryable` do Spring Framework 7 | Segunda camada de retry; ver ADR-0010 |
| Modo `adaptive` do SDK | O limitador de taxa do cliente introduz espera antes da requisição, contradizendo falhar rápido |

## Consequências

- (+) Cada chamada tem uma única camada de retry, com custo e prazo previsíveis; a leitura falha rápido e a escrita usa o
  backpressure do consumer.
- (+) Rajadas de ingestão e de consulta não competem pelas mesmas conexões.
- (-) Dois beans `DynamoDbClient`: toda injeção precisa de `@Qualifier`; os testes de configuração verificam a política de cada um.
