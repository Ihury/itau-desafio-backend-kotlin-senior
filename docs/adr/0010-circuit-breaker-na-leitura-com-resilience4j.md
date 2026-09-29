# ADR-0010: Circuit breaker na leitura com Resilience4j programático

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípio V; `specs/001-consulta-saldo/research.md` R-10 e R-11; FR-025 e SC-008

## Contexto

Com o DynamoDB indisponível ou lento, a API precisa falhar rápido e de forma explícita (503 com `Retry-After`, Problem
Details) em vez de acumular requisições presas até o timeout (Princípio V, SC-008: resposta em até 2 s). O cliente de leitura já
tem timeouts explícitos e uma única camada de retry (SDK `standard`, 2 tentativas; ADR-0009), mas isso limita o custo de cada
chamada, não impede que cada requisição gaste o orçamento inteiro de timeout enquanto a dependência está fora.

O Spring Framework 7.0.8 (verificado nos JARs) traz `org.springframework.core.retry.RetryTemplate` e, em `spring-context`,
`@Retryable`, `@ConcurrencyLimit` e `@EnableResilientMethods`, mas **não tem circuit breaker**. `@Retryable` criaria uma segunda
camada de retry (proibida pela Constitution V) e `@ConcurrencyLimit` bloqueia em vez de falhar rápido.

## Decisão

- Usar **Resilience4j 2.4.0**, apenas os módulos `resilience4j-circuitbreaker` (core, sem dependência de Spring) e
  `resilience4j-micrometer`, em **uso programático**, dentro de um decorator da porta de leitura:
  `CircuitBreakingBalanceSnapshotReader` (`adapter/output/dynamodb`) envolve o `DynamoDbBalanceSnapshotReader`. O leitor cru não é
  um bean: só existe embrulhado (composition root em `config/BalanceBeansConfig`).
- Configuração (`balance.circuit-breaker.*`, `contracts/configuration.md`): janela por tempo de 10 s, mínimo de 20 chamadas,
  abre com falha >= 50% ou chamadas lentas (> 0,5 s) >= 80%; espera em OPEN de 10 s com transição automática para HALF_OPEN;
  5 chamadas de teste em HALF_OPEN.
- Conta como falha **somente** `BalanceStoreUnavailableException`. Item encontrado, item ausente (`null`) e falhas internas
  (`IllegalStateException` de item corrompido: o banco respondeu) contam como sucesso.
- `CallNotPermittedException` (circuito aberto) é convertida em `BalanceStoreUnavailableException`, que a API traduz em
  **503 + `Retry-After`**. O valor de `Retry-After` é fixo e igual à espera em OPEN (`CircuitBreakerProperties.retryAfterSeconds`).
- As métricas `resilience4j.circuitbreaker.*` (tag `name=dynamodb-read`) são ligadas ao Micrometer por um `MeterBinder`
  (`TaggedCircuitBreakerMetrics`). A `resilience4j-micrometer` 2.4.0 é compilada contra Micrometer 1.16 e o Boot 4.1.0 usa 1.17:
  a compatibilidade é coberta por teste (`ResilienceConfigTest`).
- **A escrita (consumer) não tem circuit breaker**: o backpressure com pausa do container e backoff exponencial já cumpre esse
  papel e mantém as mensagens no broker (Princípio V).

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| `resilience4j-spring-boot4` | Compilado contra Boot 4.0.0/Spring 7.0.2 (não 4.1.0), traz auto-configuração e AOP por anotação e propriedades (comportamento implícito) que não são necessários; o core é biblioteca pura, sem acoplamento à versão do Spring |
| Circuit breaker próprio (cerca de 80 linhas) | Reimplementaria janela deslizante, half-open e métricas, com menos evidência do que uma biblioteca madura |
| Spring Cloud CircuitBreaker | Abstração desnecessária para um único breaker |
| `@Retryable`/`@ConcurrencyLimit` do Spring 7 | Segunda camada de retry (proibida) e bloqueio em vez de falha rápida; o pool com timeout de aquisição de 0,3 s já é o bulkhead |
| `Retry-After` dinâmico | O Resilience4j não expõe barato o tempo restante em OPEN; o valor fixo (espera em OPEN) é um limite superior honesto |
| Sem circuit breaker | Cada requisição gastaria o orçamento de timeout com a dependência fora; a Constitution V o exige na API |

## Consequências

- (+) Falha rápida e explícita com o DynamoDB fora (SC-008) e alívio da dependência durante a recuperação (HALF_OPEN limitado).
- (+) O core do Resilience4j não amarra o serviço a versões do Spring; o decorator é testável sem infraestrutura, com um
  delegate falso e uma configuração minúscula.
- (-) Duas dependências novas de runtime (`resilience4j-circuitbreaker`, `resilience4j-micrometer`); a segunda traz, como
  dependências de runtime não usadas, os módulos `bulkhead`, `retry`, `ratelimiter` e `timelimiter`.
- (-) O `Retry-After` fixo pode ser maior que o tempo real até o circuito tentar fechar (limite superior).
- (-) Risco residual R1: `resilience4j-micrometer` compilada com Micrometer 1.16 rodando em 1.17; mitigado por teste e a ser
  reexaminado a cada atualização do Boot.
