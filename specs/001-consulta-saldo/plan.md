# Implementation Plan: Consulta de Saldo

**Branch**: `001-consulta-saldo` | **Date**: 2026-09-29 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-consulta-saldo/spec.md` (inclui `## Clarifications` da sessão 2026-09-29)

**Note**: This template is filled in by the `/speckit-plan` command; its definition describes the execution workflow.

## Summary

Serviço de missão crítica do core banking com duas responsabilidades sobre um único modelo de dados, o **snapshot de saldo por conta**:

1. **Ingestão**: consome `transacoes-financeiras-processadas` (at-least-once, sem ordem, sem chave) e mantém, por conta, o snapshot do evento de
   maior precedência `(timestamp µs, transactionId)`. A arbitragem é feita **pelo DynamoDB** com uma única `UpdateItem` condicional
   (`ReturnValuesOnConditionCheckFailure=ALL_OLD` classifica `duplicado` x `obsoleto` sem leitura extra): convergência independente de ordem,
   duplicação e paralelismo, sem read-modify-write nem locks. Mensagens inválidas vão ao `.DLT` com motivo enumerado nos headers e bytes
   originais preservados; falhas transitórias do armazenamento geram *backpressure* (pausa + backoff exponencial com jitter) sem perder nem
   isolar mensagens válidas.
2. **Consulta**: `GET /balances/{accountId}` lê o snapshot com `ConsistentRead`, responde 200/400/404/409 (`conta-desabilitada`)/503 (+`Retry-After`,
   circuit breaker) em Problem Details (RFC 9457), com `BigDecimal` e microssegundos preservados.

Abordagem técnica (detalhada em [research.md](./research.md), com evidência empírica de spikes contra DynamoDB Local 3.3.0 e Redpanda v26.1.14):
arquitetura hexagonal em um novo bounded context `balance` (o exemplo `hello` é removido e o teste Konsist é generalizado); tabela
`AccountBalances` com `pk=ACCOUNT#<id>`/`sk=BALANCE`, sem GSI e sem ledger; dinheiro como `BigDecimal` (`N` no banco; escala completada às casas da moeda só na resposta, sem arredondar); consumer por registro com
bytes verbatim, classificação permanente/transitória/não classificada e uma única camada de retry por chamada (escrita: error handler do
consumer; leitura: SDK `standard`); circuit breaker Resilience4j (core, uso programático) na leitura; observabilidade com Actuator em porta
separada, Micrometer/Prometheus e logs JSON nativos; testes TDD com propriedade (kotest-property), contrato fake x DynamoDB Local e integração real.

## Technical Context

**Language/Version**: Kotlin 2.3.21 (JVM toolchain Java 21, Temurin)

**Primary Dependencies**: Spring Boot 4.1.0 (Spring Framework 7.0.8, Spring MVC), Spring Kafka 4.1.0 (kafka-clients 4.2.1), AWS SDK for Java v2 2.46.7
(`dynamodb`, cliente síncrono `apache5-client`), Jackson 3.1.4 (`tools.jackson`), Micrometer 1.17.0 + `micrometer-registry-prometheus`,
Spring Boot Actuator. **Novas**: `io.github.resilience4j:resilience4j-circuitbreaker` 2.4.0 e `resilience4j-micrometer` 2.4.0 (main);
`io.kotest:kotest-property` 6.2.5 e `org.awaitility:awaitility-kotlin` 4.3.0 (test) — versões e evidências em `research.md` seção 1

**Storage**: Amazon DynamoDB (DynamoDB Local 3.3.0 em desenvolvimento/CI). Tabela única `AccountBalances`, on-demand, PK `pk`/SK `sk`, sem GSI/TTL

**Testing**: JUnit Jupiter 6.0.3 + kotlin-test, Mockito, MockMvc (`@WebMvcTest`), Konsist 0.17.3 (arquitetura), kotest-property (propriedade),
Awaitility; `integrationTest` contra DynamoDB Local + Redpanda reais (docker compose); JaCoCo 0.8.12 com gate de 90% de instruções

**Target Platform**: Linux container (`eclipse-temurin:21-jre`, processo não-root), múltiplas instâncias atrás de um balanceador; Redpanda/Kafka e DynamoDB gerenciados em produção

**Project Type**: web-service (API REST síncrona + consumer Kafka assíncrono no mesmo processo), módulo Gradle único

**Performance Goals**: carga de referência (a confirmar com o cliente) 1.000 eventos/s na ingestão e 500 consultas/s; consulta p50 <= 50 ms e p99 <= 300 ms (SC-001);
saldo consultável em <= 5 s (p95) / <= 15 s (p99) após a publicação (SC-002); backlog totalmente drenado em <= 5 min após restabelecer o armazenamento (SC-007)

**Constraints**: 503 em <= 2 s com o armazenamento fora (SC-008, `apiCallTimeout` 1,5 s); nenhuma suposição de ordem do broker; sem read-modify-write nem locks locais;
suíte unitária sem infraestrutura; o enunciado do desafio não é versionado; sem dados pessoais/saldos em logs; timeouts explícitos em toda chamada remota

**Scale/Scope**: 1 bounded context, 1 endpoint REST, 1 consumer Kafka (1 tópico de entrada + 1 DLT), 1 tabela DynamoDB, 1 item por conta; ~1 milhão+ de contas, 12 partições, 2+ instâncias

Todas as incógnitas do template foram resolvidas em `research.md`; **nenhum `NEEDS CLARIFICATION` remanescente**. Decisões que dependem do usuário estão marcadas como
"decisão proposta — requer validação" (`research.md` seção 5).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Avaliação pré-pesquisa (Phase 0) contra `.specify/memory/constitution.md` v1.0.0:

| Princípio / regra | Gate | Status pré |
|-------------------|------|------------|
| I. Arquitetura hexagonal verificada | Domain Kotlin puro; `application` só domain+port; adapters por tecnologia; Konsist cobrindo **todos** os pacotes | PASS (o teste atual só cobre `hello..`: a generalização é tarefa obrigatória do plano) |
| II. Corretude sob concorrência/desordem | Escrita condicional atômica no banco; precedência pelo evento; sem locks; sem suposição de ordem; obsoleto sem erro | PASS (a ser demonstrado por R-04) |
| III. Idempotência e at-least-once | Commit após persistir; sem auto-commit; erros classificados; DLT com motivo; mensagem inválida não bloqueia nem some | PASS (R-07, R-08) |
| IV. Integridade financeira | `BigDecimal` ponta a ponta; sem perda de escala/precisão; ISO 4217; µs; ISO 8601 com offset; validação na borda | PASS (R-06) |
| V. Resiliência com limites | Timeouts explícitos; **uma** camada de retry por chamada; circuit breaker + 503/`Retry-After`; backpressure no consumer | PASS (R-08, R-10, R-11) |
| VI. Test-first e evidência | TDD; cenários obrigatórios; propriedade de convergência; `test` sem infra; `integrationTest` real; JaCoCo >= 90% | PASS (R-14; correção do contexto de teste que conecta ao Kafka) |
| VII. Observabilidade | Logs JSON com `accountId`/`transactionId`/correlação, sem saldo/PII; métrica por desfecho e latências; liveness x readiness; saúde das dependências críticas exposta em grupo separado + gauge | PASS (R-13; a readiness não inclui o DynamoDB por decisão do usuário) |
| VIII. Simplicidade e decisões registradas | Sem componente sem requisito (GSI, ledger, cache); ADRs; o não feito documentado | PASS (R-03, R-17; ADRs listados abaixo) |
| Restrições técnicas | Stack base mantida; dependências novas com compatibilidade comprovada e versão fixa; tags Docker fixas; 12-factor; container não-root/`MaxRAMPercentage`/graceful/healthcheck; `accountId` validado; Problem Details; OpenAPI; enunciado fora do repo | PASS (versões comprovadas em `research.md` seção 1) |
| Fluxo e quality gates | Constitution Check no plan; violações em Complexity Tracking; gates `check` + `integrationTest` + imagem + CodeQL | PASS |

**Resultado pré-pesquisa: aprovado, sem violações.**

### Re-avaliação pós-design (Phase 1)

Reavaliado após `data-model.md`, `contracts/*` e `quickstart.md`:

| Princípio | Evidência no design | Status pós |
|-----------|--------------------|------------|
| I | Estrutura de pacotes e regras Konsist (a)-(e) em R-01; portas separadas por papel (R-02); métricas e relógio por port/`Clock` (application sem Micrometer) | PASS |
| II | `UpdateItem` + `ConditionExpression` (`data-model.md` 4.4); precedência `(ts, txId)` com ordem lexicográfica canônica (2.1); spike: 400 escritas concorrentes convergem; 0 divergências de ordem em 2.000 UUIDs | PASS |
| III | `ByteArrayDeserializer`, `AckMode=BATCH`, `enable.auto.commit=false`; DLT com destino sem partição fixa; DLT fora = não confirma (validado no spike) | PASS |
| IV | `BigDecimal` ponta a ponta com `N` no banco (valor exato; zeros à direita normalizados pelo DynamoDB, escala da resposta completada às casas da moeda sem arredondar; teto de 38 dígitos imposto no domínio); `WRITE_BIGDECIMAL_AS_PLAIN`; parser sem coerção; µs em `Long`; `updated_at` reproduz o exemplo do cliente | PASS |
| V | Tabela de timeouts/retry por cliente (R-10); CB Resilience4j na leitura; consumer sem segunda camada; pools isolados | PASS |
| VI | Plano de testes em 8 camadas (R-14); propriedade com seed fixa; contrato fake x DynamoDB Local; chaos por `docker compose pause`; cenários obrigatórios mapeados em `quickstart.md` | PASS |
| VII | `contracts/observability.md` (métricas, health, logs); rejeição contada apenas após DLT confirmar; `balance.events{outcome,reason}` reconcilia com o consumido | PASS |
| VIII | Ledger/GSI/coalescência/OTel/springdoc recusados com motivação (R-03, R-09, R-12, R-16, R-17); 15 ADRs planejados | PASS |

**Resultado pós-design: aprovado, sem violações.** As escolhas que divergem da diretriz do arquiteto ou aumentam a complexidade estão em Complexity Tracking.

## Project Structure

### Documentation (this feature)

```text
specs/001-consulta-saldo/
├── plan.md                          # This file (/speckit-plan command output)
├── research.md                      # Phase 0 output (/speckit-plan command)
├── data-model.md                    # Phase 1 output (/speckit-plan command)
├── quickstart.md                    # Phase 1 output (/speckit-plan command)
├── contracts/                       # Phase 1 output (/speckit-plan command)
│   ├── openapi.yaml                 #   API REST (OpenAPI 3.1, contract-first; validado)
│   ├── transaction-event.schema.json#   Evento de entrada (contrato imposto pelo cliente)
│   ├── kafka-events.md              #   Tópicos, validação, motivos, contrato do DLT
│   ├── observability.md             #   Métricas, health checks, logs
│   └── configuration.md             #   Variáveis de ambiente
├── checklists/requirements.md       # (/speckit-specify)
└── tasks.md                         # Phase 2 output (/speckit-tasks command - NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
src/main/kotlin/br/com/itau/challenge/
├── Application.kt                                   # inalterado
└── balance/                                         # bounded context (o pacote `hello` é removido)
    ├── domain/
    │   ├── model/                                   # AccountId, TransactionId, OwnerId, CurrencyCode, Money, EventInstant,
    │   │                                            # AccountStatus, TransactionType, TransactionStatus, Transaction, AccountState,
    │   │                                            # TransactionEvent, Precedence, BalanceSnapshot, ApplyResult, RejectionReason
    │   └── exception/                               # InvalidEventException(reason), AccountNotFoundException, AccountDisabledException,
    │                                                # BalanceStoreUnavailableException (transitória), BalanceStoreRejectedException
    ├── port/
    │   ├── input/                                   # ProcessTransactionEventUseCase, GetBalanceUseCase
    │   └── output/                                  # BalanceSnapshotWriter, BalanceSnapshotReader, ProcessingMetrics
    ├── application/                                 # ProcessTransactionEventService (tolerância de futuro com Clock), GetBalanceService
    ├── adapter/
    │   ├── input/
    │   │   ├── web/                                 # BalanceController, dto/BalanceResponse, ProblemDetailsAdvice, CorrelationIdFilter
    │   │   └── kafka/                               # TransactionEventListener, TransactionEventParser (estrito), DeadLetterConfig
    │   │                                            #   (DefaultErrorHandler, recoverer, backoffs, pause handler), RejectionHeaders
    │   └── output/
    │       ├── dynamodb/                            # DynamoDbClientsConfig (read/write), DynamoDbBalanceSnapshotWriter,
    │       │                                        # DynamoDbBalanceSnapshotReader, BalanceItemMapper, CircuitBreakingBalanceSnapshotReader,
    │       │                                        # DynamoDbHealthIndicator
    │       └── metrics/                             # MicrometerProcessingMetrics
    └── config/                                      # composition root: @ConfigurationProperties, Clock, ObjectMapper do parser,
                                                     # circuit breaker registry, beans de serviço (nada depende deste pacote)
src/main/resources/
├── application.yaml                                 # propriedades por env (contracts/configuration.md)
└── static/openapi.yaml                              # cópia servida do contrato (contracts/openapi.yaml)

src/test/kotlin/br/com/itau/challenge/
├── ArchitectureTest.kt                              # Konsist generalizado (todos os contextos) — substitui HexagonalArchitectureTest
├── ApplicationTests.kt                              # @ActiveProfiles("test"): contexto sobe sem broker
└── balance/                                         # espelha main: domain, application, adapter/*, testing/InMemoryBalanceStore,
                                                     # BalanceSnapshotWriterContract (abstrato), ConvergencePropertyTest (kotest-property),
                                                     # OpenApiContractTest (anti-drift)
src/test/resources/application-test.yaml
src/integrationTest/kotlin/br/com/itau/challenge/balance/   # ingestão ponta a ponta, DLT, concorrência real, outage (docker pause),
                                                            # DynamoDbBalanceSnapshotWriterContractIT, métricas do circuit breaker

infra/dynamodb/{seed.sh, account-balances.json}     # tabela AccountBalances + conta de exemplo (substitui GreetingMessages)
infra/redpanda/{config.sh, seed.sh, produce-transactions-events.sh (inalterado), produce-scenario-events.sh (novo)}
docker-compose.yml, Makefile, Dockerfile, .github/workflows/*   # ajustes descritos em R-15; CI segue verde
http/balances.http                                   # substitui http/hello.http
docs/adr/0001..0015-*.md                             # ADRs (lista abaixo)
perf/k6-balance-read.js                              # opcional (R-14)
```

**Structure Decision**: módulo Gradle único (o starter-kit é monomódulo) com um novo bounded context `balance` seguindo `domain/port/application/adapter`, o mesmo layout
do exemplo `hello` do starter. Adapters isolados por tecnologia (`input/web`, `input/kafka`, `output/dynamodb`, `output/metrics`); o pacote `config` é o
*composition root* e fica fora das quatro camadas (regra Konsist: nenhuma camada depende dele). Sem novos módulos, serviços ou repositórios: o requisito
é um serviço único com um consumer e uma API sobre o mesmo modelo.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

O Constitution Check **não tem violações**. Registram-se, por transparência, as escolhas que aumentam a complexidade ou divergem da diretriz do arquiteto:

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Duas dependências novas de runtime/teste (Resilience4j, kotest-property) | Constitution V exige circuit breaker e VI exige teste de propriedade; o Spring Framework 7 tem `@Retryable`/`@ConcurrencyLimit` mas **nenhum** circuit breaker | Circuit breaker próprio reimplementaria janela deslizante, half-open e métricas; teste de propriedade manual não tem *shrinking* |
| Dois `DynamoDbClient` (leitura e escrita) | Estratégia de retry é por cliente (o override por requisição só cobre timeouts); leitura precisa falhar em <= 2 s, escrita faz backpressure; pools isolados | Um cliente único forçaria retry duplicado ou compartilhamento de pool entre ingestão e API |
| Sem `ErrorHandlingDeserializer` (diretriz sugeria) | `ByteArrayDeserializer` nunca lança e preserva os bytes originais no DLT; o parser próprio produz os 7 motivos | Delegar ao `ErrorHandlingDeserializer(JsonDeserializer)` perderia motivos finos e bytes originais |
| Pacote `config` fora das quatro camadas | Composition root (propriedades, `Clock`, registry do CB, beans) | Espalhar `@Bean` pelas camadas acoplaria `application` ao Spring além do `@Service` do starter |
| `@Service` na camada `application` | Convenção do starter-kit; limitada por regra Konsist ao stereotype | `application` 100% sem Spring divergiria do starter sem ganho verificável |
| Remoção do exemplo `hello` (*decisão proposta — requer validação*) | Ruído e risco (listener de outro tópico, tabela sem uso) num serviço de core banking; Konsist precisa cobrir todos os contextos | Manter `hello` exige proteger um contexto irrelevante e confunde o avaliador |

## ADRs planejados (`docs/adr/`, escritos na implementação)

Título e decisão em uma linha (contexto/alternativas/trade-offs completos em `research.md`):

1. **0001 Arquitetura hexagonal por bounded context** — contexto `balance`, `hello` removido, Konsist cobre todos os contextos e imports proibidos.
2. **0002 Modelagem DynamoDB** — `AccountBalances`, `pk=ACCOUNT#id`/`sk=BALANCE`, on-demand, sem GSI nem ledger na v1.
3. **0003 Precedência e escrita condicional atômica** — `(timestamp µs, txId)` via `UpdateItem`+`ConditionExpression`; sem RMW nem locks locais.
4. **0004 Duplicado x obsoleto** — `ALL_OLD`; duplicado = igual ao snapshot vigente; anomalia quando o conteúdo diverge.
5. **0005 Dinheiro e tempo** — `BigDecimal` ponta a ponta, `N` no DynamoDB, escala completada às casas da moeda só na resposta (sem arredondar); µs `Long`; `updated_at` ISO com offset `America/Sao_Paulo`.
6. **0006 Validação estrita e catálogo de motivos** — parser de árvore no adapter; 7 motivos + `unprocessable_event`; futuro 5 min; mínimo 2000-01-01 (`transaction.timestamp`) e 1900-01-01 (`account.created_at`).
7. **0007 Consumer at-least-once e particionamento** — listener por registro, bytes verbatim, commit em lote pós-persistência, 12/3 partições.
8. **0008 Transitório x permanente, backpressure e DLT** — não-retentável -> DLT; transitório -> backoff+jitter com pausa; DLT fora = não confirma.
9. **0009 Uma camada de retry e clientes separados** — escrita: só o consumer; leitura: SDK `standard` (2 tentativas); timeouts/pools isolados.
10. **0010 Circuit breaker na leitura (Resilience4j programático)** — core 2.4.0 num decorator do port; 503 + `Retry-After`.
11. **0011 Leitura fortemente consistente** — `ConsistentRead=true` (flag) para atender FR-027.
12. **0012 API, Problem Details e OpenAPI contract-first** — `type` URN estável; 400/404/409/503/500; sem springdoc.
13. **0013 Observabilidade** — Actuator em porta separada; readiness = só estado do app; dependências em `/actuator/health/dependencies` + gauge (sem tirar a instância de rotação); Micrometer/Prometheus; desfecho único por mensagem; logs JSON com MDC; tracing = evolução.
14. **0014 Estratégia de testes** — TDD, kotest-property, contrato fake x DynamoDB Local, integração via compose, chaos por `docker compose pause`, gate 90%.
15. **0015 Empacotamento e operação** — Dockerfile não-root, `MaxRAMPercentage`, healthcheck, graceful shutdown, tags fixas.

## Respostas aos critérios de avaliação do cliente

| Critério | Resposta do plano | Onde |
|----------|-------------------|------|
| Modelagem DynamoDB | PK `ACCOUNT#<id>`, SK `BALANCE`, um item por conta; sem GSI (nenhum padrão de acesso); ledger e GSI por titular documentados como evolução | `data-model.md` 4, R-03 |
| Concorrência | `UpdateItem` condicional atômico com precedência `(ts, txId)`; propriedade de convergência + teste concorrente real | R-04, R-14 |
| Resiliência | Backpressure com backoff exponencial+jitter no consumer; SDK `standard` com timeouts na leitura; circuit breaker + 503/`Retry-After` | R-08, R-10, R-11 |
| Testes | TDD; unitários sem infra; propriedade; integração DynamoDB Local + Redpanda; DLT; outage; contrato HTTP; gate 90% | R-14, `quickstart.md` |
| Qualidade / hexagonal | Contexto `balance`, portas por papel, Konsist generalizado com imports proibidos | R-01, R-02 |
| Cenários adversos | 7 motivos de rejeição, mensagens gigantes/binárias, UUID lenient, `1E999999999`, timestamp futuro, DLT fora, DynamoDB fora | `contracts/kafka-events.md`, `quickstart.md` |
| Production readiness | Logs JSON com MDC, métricas por desfecho, liveness/readiness/dependências separados, Dockerfile não-root, graceful shutdown | R-13, R-15, `contracts/` |
| O que não coube | Ledger, GSI, sharding, coalescência, OTel, IaC, k6 sustentado — cada um com motivação e desenho | R-17 |
