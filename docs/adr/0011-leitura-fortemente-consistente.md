# ADR-0011: Leitura fortemente consistente do snapshot

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípios IV e V; `specs/001-consulta-saldo/research.md` R-05; `data-model.md` seção 4.5; FR-027 e FR-030

## Contexto

A consulta `GET /balances/{accountId}` deve refletir o snapshot "mais recente já efetivado" (FR-027): depois que a ingestão
confirma a escrita de um evento, a consulta seguinte não pode devolver o estado anterior. O `GetItem` do DynamoDB é
eventualmente consistente por padrão e, logo após uma escrita, pode devolver o item antigo por uma janela de milissegundos a
cerca de 1 s. Saldo desatualizado sem sinalização é pior que erro (Princípio V).

## Decisão

- O `GetItem` do adapter `DynamoDbBalanceSnapshotReader` usa `ConsistentRead=true` por padrão.
- A escolha é configurável por `DYNAMODB_READ_CONSISTENT` (`dynamodb.read.consistent`), com default `true`. Desligá-la é uma
  decisão de negócio (aceitar defasagem de cerca de 1 s em troca de metade do custo de leitura), nunca um padrão silencioso.
- Se o DynamoDB não consegue servir a leitura forte (por exemplo, partição de rede), a falha vira
  `BalanceStoreUnavailableException` e, na API, 503 com `Retry-After`: nunca `null`/404 nem saldo presumido (FR-030).

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| Leitura eventualmente consistente | Metade do custo (0,5 RCU), mas contradiz FR-027: a consulta imediata a um evento recém-consumido poderia mostrar o saldo anterior. Fica disponível por flag caso o cliente aceite a defasagem |
| Cache local com TTL | Serve saldo desatualizado sem sinalização, incompatível com o Princípio V; cada instância teria uma visão diferente; sem requisito que o justifique (Princípio VIII) |
| Ler da réplica/GSI | Não há GSI (nenhum padrão de acesso o exige) e GSIs são sempre eventualmente consistentes |

## Consequências

- (+) A consulta reflete a última escrita efetivada em qualquer instância (FR-027), sem lógica adicional na aplicação.
- (-) Custo de leitura 2x: 1 RCU por consulta de item com cerca de 300 B (aproximadamente 500 RCU/s na carga de referência de
  500 consultas/s), aceito para cumprir FR-027.
- (-) A leitura forte não está disponível durante falhas de partição do DynamoDB; isso é comportamento desejado (503 explícito,
  circuit breaker da ADR-0010) e não uma regressão.
- (-) O limite de throughput por partição (cerca de 3.000 RCU/s por chave, segundo a documentação da AWS) vale para leituras
  fortes; uma conta consultada acima disso teria throttling, tratado como indisponibilidade transitória.
