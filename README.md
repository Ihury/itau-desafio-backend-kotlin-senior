# Consulta de Saldo

[![Build](../../actions/workflows/build.yml/badge.svg)](../../actions/workflows/build.yml)
[![Test & Coverage](../../actions/workflows/test.yml/badge.svg)](../../actions/workflows/test.yml)
[![Docker](../../actions/workflows/docker.yml/badge.svg)](../../actions/workflows/docker.yml)
[![CodeQL](../../actions/workflows/codeql.yml/badge.svg)](../../actions/workflows/codeql.yml)

Serviço (Kotlin, Spring Boot 4, DynamoDB e Kafka) que mantém, por conta, o **snapshot de saldo da transação mais recente** recebida por
eventos e o expõe em `GET /balances/{accountId}`. A corretude não depende de ordem, unicidade nem de uma única instância: o serviço
converge para o mesmo saldo com eventos fora de ordem, duplicados, reentregues ou processados em paralelo.

O desenho foi conduzido por especificação (GitHub Spec Kit): tudo o que foi decidido, e por quê, está versionado em
[`specs/001-consulta-saldo/`](specs/001-consulta-saldo/) (spec, plano, pesquisa, contratos, tarefas) e nos
[15 ADRs](#decisões-de-arquitetura-adrs).

## Sumário

- [Avaliação em 10 minutos](#avaliação-em-10-minutos)
- [Como rodar](#como-rodar)
- [Como testar](#como-testar)
- [API](#api)
- [Mensageria Kafka](#mensageria-kafka)
- [Arquitetura hexagonal](#arquitetura-hexagonal)
- [Modelagem no DynamoDB](#modelagem-no-dynamodb)
- [Concorrência e ordem](#concorrência-e-ordem)
- [Resiliência](#resiliência)
- [Observabilidade e operação](#observabilidade-e-operação)
- [Configuração](#configuração)
- [Decisões de arquitetura (ADRs)](#decisões-de-arquitetura-adrs)
- [O que NÃO foi feito e por quê](#o-que-não-foi-feito-e-por-quê)
- [Riscos conhecidos e o que não foi verificado](#riscos-conhecidos-e-o-que-não-foi-verificado)
- [Uso de IA](#uso-de-ia)
- [Stack e imagens Docker](#stack-e-imagens-docker)

## Avaliação em 10 minutos

Pré-requisito único: **Docker** (com Docker Compose) e `make` (no Windows, use o WSL2).

```bash
make up                                                        # app + DynamoDB Local + Redpanda + seeds (a 1a vez baixa as imagens)
make balance-get ACCOUNT=5b19c8b6-0cc4-4c72-a989-0c2ee15fa975  # 200 com a conta de exemplo do seed
make kafka-produce-scenario                                    # desordem, duplicata, empate, DISABLED, inválidas, veneno binário
make balance-get ACCOUNT=00000000-0000-4000-8001-000000000001  # 300.00 (o script imprime o resultado esperado de cada conta)
curl -s localhost:8082/actuator/health/readiness               # {"status":"UP"}
make stop                                                      # derruba tudo
```

Para ver o comportamento sob falha do armazenamento: `make chaos-dynamodb-pause`, consulte uma conta (503 com `Retry-After`
em ~1 s, readiness continua 200) e `make chaos-dynamodb-unpause` (o consumer retoma sozinho, sem perda). O roteiro completo, com
o resultado esperado de cada passo, está em [`quickstart.md`](specs/001-consulta-saldo/quickstart.md).

## Como rodar

```bash
make up      # sobe tudo em background; o app só inicia depois que a tabela e os tópicos existem (seeds concluídos)
make logs    # logs JSON do app
make stop    # derruba tudo (apenas os containers deste projeto)
make help    # todos os alvos
```

| Serviço | Endereço | Observação |
|-|-|-|
| API | http://localhost:8080 | `GET /balances/{accountId}`, `GET /openapi.yaml` |
| Gerenciamento (Actuator) | http://localhost:8082 | saúde e métricas; **não rotear pelo balanceador público** |
| DynamoDB Local | http://localhost:8000 | modo in-memory |
| DynamoDB Admin | http://localhost:8001 | inspeção da tabela |
| Redpanda Console | http://localhost:8081 | inspeção de tópicos e do DLT |
| Redpanda (Kafka) | localhost:19092 | listener externo |

Alvos úteis:

| Alvo | O que faz |
|-|-|
| `make balance-get ACCOUNT=<uuid>` | Consulta um saldo (valida o formato do UUID antes de chamar) |
| `make kafka-produce-scenario` | Publica o cenário determinístico e imprime os resultados esperados |
| `make kafka-produce-transactions-events TOPIC=<t> COUNT=n` | Gera `n` eventos aleatórios |
| `make kafka-consume TOPIC=<t>` | Lê um tópico (ex.: `transacoes-financeiras-processadas.DLT`) |
| `make db-scan` | Lista os itens da tabela `AccountBalances` |
| `make chaos-dynamodb-pause` / `chaos-dynamodb-unpause` | Congela e retoma o DynamoDB Local |
| `make http` | Executa `http/*.http` contra o app (via Docker) |
| `make clean-containers` | Remove todos os containers deste projeto, inclusive órfãos |

**Desenvolvimento pela IDE:** `make db-up kafka-up wait-seeds` sobe só a infraestrutura e espera os seeds; depois rode
`Application.kt` ou `./gradlew bootRun` (os defaults do `application.yaml` já apontam para `localhost`).

**Problemas comuns:** `port is already allocated` (a stack usa 8080, 8082, 8000, 8001, 8081 e 19092); a primeira subida baixa
~6 imagens; se algo ficar inconsistente, `make clean-containers` recomeça do zero.

## Como testar

| Comando | O que roda | Infraestrutura |
|-|-|-|
| `./gradlew check` | Testes unitários (395), teste de arquitetura Konsist, propriedade de convergência e **gate JaCoCo >= 90%** (hoje 97,1%) | Nenhuma (o contexto Spring de teste sobe sem broker nem banco) |
| `make test` | O mesmo `check`, dentro de um container (estágio `test` do Dockerfile), como no CI | Só Docker |
| `make integration-test` | 66 testes de integração contra DynamoDB Local e Redpanda **reais**: ingestão ponta a ponta, concorrência real (32 threads na mesma conta), DLT por motivo, indisponibilidade do armazenamento, métricas e saúde, reinício gracioso | Compose (sobe e espera os seeds; sempre reexecuta) |

Pontos que sustentam a confiança na corretude (detalhes no [ADR-0014](docs/adr/0014-estrategia-de-testes-e-evidencia-de-corretude.md)):

- **TDD com evidência de vermelho**: os testes de integração escritos depois do código foram verificados por mutação temporária
  (por exemplo, trocar a condição da escrita por `>` ou por "último a chegar vence") e o resultado consta nas notas de execução do
  [`tasks.md`](specs/001-consulta-saldo/tasks.md).
- **Teste de propriedade** de convergência (kotest-property): qualquer ordem, duplicata ou entrega repetida converge para o evento
  de maior precedência; o teste é reexecutado contra o DynamoDB Local e prova que **detecta** uma implementação ingênua
  (last-write-wins).
- **Teste de contrato** do writer: o mesmo conjunto de casos roda contra o fake em memória e contra o DynamoDB Local.
- **Konsist** em todo commit: domínio puro, `application` só com domínio, portas e `@Service`, adapters isolados por tecnologia,
  ninguém depende de `config`, nenhum `catch` engole exceção sem log, métrica ou `throw`.
- **Teste anti-drift do OpenAPI** contra as respostas reais e **teste de privacidade de logs** com valores sentinela.
- O caos usa `docker compose pause dynamodb` (o teste sempre desfaz o `pause`).

## API

`GET /balances/{accountId}` (contrato completo em [`openapi.yaml`](specs/001-consulta-saldo/contracts/openapi.yaml), também servido em
`GET /openapi.yaml`).

```bash
curl -i localhost:8080/balances/5b19c8b6-0cc4-4c72-a989-0c2ee15fa975
```

```json
{"id":"5b19c8b6-0cc4-4c72-a989-0c2ee15fa975","owner":"315e3cfe-f4af-4cd2-b298-a449e614349a","balance":{"amount":183.12,"currency":"BRL"},"updated_at":"2025-07-05T18:04:13.433-03:00"}
```

- O saldo é um número JSON **exato** (nunca `double`, nunca arredondado, sem notação científica). A escala é completada às casas da
  moeda na resposta (`183.1` vira `183.10`; `10.123` permanece `10.123`).
- `updated_at` é o instante do **evento** que originou o snapshot (não o do processamento), com microssegundos e offset de
  `America/Sao_Paulo` (configurável).
- Respostas de erro em `application/problem+json` (RFC 9457) com `type` estável, sem pilha nem nomes de infraestrutura:

| Situação | Status | `type` (`urn:problem-type:consulta-saldo:...`) |
|-|-|-|
| `accountId` não é UUID canônico (`abc`, `1-1-1-1-1`) | 400 | `requisicao-invalida` (o banco nem é consultado) |
| Conta sem nenhum evento processado | 404 | `conta-nao-encontrada` (nunca saldo zerado) |
| Conta cujo snapshot vigente é `DISABLED` | 409 | `conta-desabilitada` (sem saldo nem titular no corpo) |
| Armazenamento indisponível, lento ou circuit breaker aberto | 503 | `servico-indisponivel` + `Retry-After: 10` |
| Falha interna (inclui item corrompido no banco) | 500 | `erro-interno` (detalhe só no log) |

- `X-Correlation-Id` (`[A-Za-z0-9._-]{1,64}`) é aceito ou gerado, devolvido em toda resposta e presente nos logs.
- A leitura é **fortemente consistente** por padrão ([ADR-0011](docs/adr/0011-leitura-fortemente-consistente.md)); a falha de leitura
  nunca vira 404 nem saldo presumido.

## Mensageria Kafka

| Tópico | Papel | Partições |
|-|-|-|
| `transacoes-financeiras-processadas` | Entrada: um evento de transação com o estado da conta; mensagens sem chave, **nenhuma ordem assumida** | 12 |
| `transacoes-financeiras-processadas.DLT` | Isolamento de mensagens inválidas (retenção de 14 dias) | 3 |

- **At-least-once**: o container só confirma o offset depois que o listener persistiu; sem auto-commit. A idempotência vem da escrita
  condicional no banco. O consumer trabalha sobre bytes verbatim (`ByteArrayDeserializer`) e valida com um parser estrito.
- **Mensagem inválida vai para o DLT** com o valor original intacto (inclusive binário) e os headers `x-rejection-reason`,
  `x-rejection-detail` (só o caminho do campo, nunca valores) e `x-rejected-at`. Motivos (conjunto fechado):
  `malformed_payload`, `missing_field`, `invalid_identifier`, `invalid_value`, `invalid_currency`, `invalid_timestamp`,
  `unknown_domain_value` e, para defeito interno, `unprocessable_event`. Os headers de exceção do Spring são excluídos de propósito
  (mensagens de parser poderiam vazar saldo ou titular).
- **Falha do armazenamento nunca manda mensagem válida ao DLT**: a mensagem fica no broker e o consumer aplica backpressure.
- **Reprocessamento do DLT é manual** nesta versão; o roteiro está em
  [`kafka-events.md`](specs/001-consulta-saldo/contracts/kafka-events.md) (seção 7). O contrato do evento:
  [`transaction-event.schema.json`](specs/001-consulta-saldo/contracts/transaction-event.schema.json).

## Arquitetura hexagonal

O núcleo (`domain`) não conhece Spring, AWS, Kafka nem Jackson. Tudo que é externo entra por **portas** implementadas por
**adapters**; as dependências apontam sempre para o domínio. A regra é **verificada por teste** (Konsist), não por convenção
([ADR-0001](docs/adr/0001-arquitetura-hexagonal-por-bounded-context.md)).

```mermaid
graph TD
    Adapter["adapter<br/>(input/web, input/kafka, output/dynamodb, output/metrics)"]
    Application["application<br/>(casos de uso)"]
    Port["port<br/>(input/output)"]
    Domain["domain<br/>(modelos, regras, exceções)"]
    Config["config<br/>(composition root)"]
    Adapter --> Port
    Adapter --> Domain
    Application --> Port
    Application --> Domain
    Port --> Domain
    Config -.-> Adapter
    Config -.-> Application
```

```mermaid
flowchart LR
    Kafka(["Kafka<br/>transacoes-financeiras-processadas"]) --> Listener["TransactionEventListener<br/>+ TransactionEventParser"]
    Listener --> Process["ProcessTransactionEventService"]
    Process --> Writer["DynamoDbBalanceSnapshotWriter<br/>(UpdateItem condicional)"]
    Writer --> DB[("DynamoDB<br/>AccountBalances")]
    Listener -. inválida .-> DLT(["DLT"])

    HTTP(["GET /balances/{id}"]) --> Controller["BalanceController"]
    Controller --> Get["GetBalanceService"]
    Get --> Reader["CircuitBreakingBalanceSnapshotReader<br/>-> DynamoDbBalanceSnapshotReader"]
    Reader --> DB
```

Código em `src/main/kotlin/br/com/itau/challenge/balance/`:

| Camada | Conteúdo |
|-|-|
| `domain` | `Money` (BigDecimal exato), `EventInstant` (µs), `Precedence` (timestamp, txId), `BalanceSnapshot`, `TransactionEvent`, identificadores canônicos, `RejectionReason`, exceções |
| `port` | `GetBalanceUseCase`, `ProcessTransactionEventUseCase` (entrada); `BalanceSnapshotReader`, `BalanceSnapshotWriter`, `ProcessingMetrics` (saída) |
| `application` | `GetBalanceService` (regra de conta desabilitada), `ProcessTransactionEventService` (tolerância de futuro, desfecho único), `FutureTolerance` |
| `adapter/input/web` | `BalanceController`, `ProblemDetailsAdvice`, `CorrelationIdFilter`, `OpenApiController` |
| `adapter/input/kafka` | `TransactionEventListener`, `TransactionEventParser` (estrito), `DeadLetterConfig`, `BackpressureConfig`, `FailureClassifier` |
| `adapter/output/dynamodb` | Reader e writer, `BalanceItemMapper`, clientes separados, `CircuitBreakingBalanceSnapshotReader`, `DynamoDbHealthIndicator` |
| `adapter/output/metrics` | `MicrometerProcessingMetrics` |
| `config` | Composition root: beans, circuit breaker, propriedades |

Outros diretórios: `src/test` (unitários), `src/integrationTest` (infra real), `infra/` (seeds e scripts do compose), `http/`
(exemplos), `docs/adr/`, `docs/metodologia-ia.md`, `specs/` e `.specify/` (artefatos do Spec Kit).

## Modelagem no DynamoDB

Tabela única `AccountBalances`, on-demand, **um item por conta** com o snapshot vigente ([ADR-0002](docs/adr/0002-modelagem-dynamodb-snapshot-por-conta.md)):

| Atributo | Tipo | Conteúdo |
|-|-|-|
| `pk` (partition key) | S | `ACCOUNT#<accountId em minúsculas>` |
| `sk` (sort key) | S | constante `BALANCE` |
| `schemaVersion`, `ownerId`, `accountStatus`, `balanceCurrency` | N / S | dados do snapshot |
| `balanceAmount` | **N** | saldo exato (`BigDecimal.toPlainString()`, até 38 dígitos) |
| `accountCreatedAtMicros` | N | criação da conta (µs) |
| `lastTxTsMicros` + `lastTxId` | N + S | **chave de precedência** do evento que originou o snapshot |

- **Padrões de acesso**: (AP1) leitura por `GetItem` na chave primária, fortemente consistente; (AP2) escrita condicional por
  `UpdateItem` na mesma chave. Ambos são O(1) e atingem uma única partição por conta.
- **Por que o `sk` é constante**: o key schema é imutável; `pk`/`sk` genéricos permitem acrescentar outros tipos de item na
  partição da conta (por exemplo um ledger `TX#...`) sem migrar a tabela, com custo zero hoje.
- **Por que sem GSI**: não há padrão de acesso que o exija (só consulta por conta) e cada GSI multiplicaria o custo de escrita de
  cada evento. Uma consulta por titular seria um GSI esparso (`OWNER#<id>`), documentado como evolução.
- **Por que sem ledger**: o requisito é o saldo mais atual, não o histórico; um ledger dobraria a escrita, concentraria carga na
  partição da conta e não muda a corretude. Se surgir requisito de extrato, entra como item `TX#...` com TTL, em escrita
  independente e idempotente (nunca `TransactWriteItems`, que perderia o registro quando o snapshot fosse obsoleto).
- **Saldo como `N` e `BigDecimal`** (e não texto nem centavos inteiros): exatidão sem perda, comparável e sem assumir casas fixas
  ([ADR-0005](docs/adr/0005-representacao-de-dinheiro-e-tempo.md)).

## Concorrência e ordem

A pergunta é sempre a mesma: qual evento é o mais recente? A resposta é **determinística**, não depende de ordem de chegada nem do
relógio do servidor ([ADR-0003](docs/adr/0003-precedencia-deterministica-e-escrita-condicional-atomica.md)):

- **Precedência** = `(transaction.timestamp em µs, transaction.id)`; no empate de instante vence o maior `transaction.id`
  (comparação lexicográfica da string canônica em minúsculas, a mesma ordem que o banco usa).
- **Uma única `UpdateItem` por evento**, decidida pelo próprio DynamoDB:
  `attribute_not_exists(pk) OR lastTxTsMicros < :ts OR (lastTxTsMicros = :ts AND lastTxId < :tx)`. Sem leitura-modificação-escrita,
  sem lock local, sem transação: instâncias e threads concorrentes são arbitradas pelo banco, e a consulta nunca vê campos de
  eventos diferentes misturados (o item é atômico).
- **Duplicado x obsoleto** ([ADR-0004](docs/adr/0004-classificacao-de-desfechos-duplicado-versus-obsoleto.md)): o `ConditionalCheckFailed`
  não é erro; o item vigente vem na própria exceção (`ALL_OLD`) e classifica: chave igual = `duplicate`; precedência menor =
  `obsolete`. Mesma chave com conteúdo divergente é a anomalia `conflicting_duplicate` (defeito da origem; prevalece o primeiro e
  o contador `balance.events.anomalies` alerta).
- **Transações `DECLINED` participam da precedência**: o evento carrega o estado da conta naquele instante, e o snapshot mais
  recente é o que vale.
- **Conta `DISABLED`**: o snapshot é atualizado normalmente, mas a consulta responde 409 `conta-desabilitada`; um evento antigo não
  a reabilita e um evento mais novo com `ENABLED` sim.
- **Tolerância de timestamp futuro** configurável (`BALANCE_FUTURE_TOLERANCE`, 5 min): o relógio só valida, nunca decide precedência.
- **Custo conhecido**: um evento obsoleto ainda consome 1 WCU (a condição falsa é cobrada). Mitigação futura: coalescência por conta.

## Resiliência

| Mecanismo | O que faz | Onde |
|-|-|-|
| **DLT** | Mensagem inválida vai ao DLT com o motivo, valor original preservado; DLT fora = não confirma o offset (reentrega), nunca perde | [ADR-0008](docs/adr/0008-erros-transitorios-permanentes-backpressure-e-dlt.md) |
| **Backpressure** | Falha transitória do armazenamento (indisponível, throttling, timeout): backoff exponencial 500 ms x2 até 30 s com jitter, sem limite de tentativas, container **pausado** entre tentativas (o poll continua vivo, sem rebalance); a mensagem fica no broker e **nunca** vai ao DLT | idem |
| **Uma camada de retry por chamada** | Escrita: só o error handler do consumer (o SDK não tenta de novo). Leitura: só o SDK (1 retry). Nunca retry multiplicado | [ADR-0009](docs/adr/0009-uma-camada-de-retry-e-clientes-dynamodb-separados.md) |
| **Clientes DynamoDB separados** | Timeouts, retry e pools próprios para leitura (API) e escrita (consumer): uma rajada de ingestão não esgota as conexões da consulta | idem |
| **Circuit breaker na leitura** | Resilience4j programático: com o banco doente a API responde 503 + `Retry-After` em milissegundos em vez de acumular chamadas lentas | [ADR-0010](docs/adr/0010-circuit-breaker-na-leitura-com-resilience4j.md) |
| **Falha não classificada** | Defeito interno determinístico: 3 entregas e DLT `unprocessable_event`, para não bloquear a partição para sempre | ADR-0008 |
| **Encerramento gracioso** | `SIGTERM` -> termina o registro corrente, o que não foi persistido não é confirmado e é reentregue | [ADR-0013](docs/adr/0013-observabilidade.md), [ADR-0015](docs/adr/0015-empacotamento-e-operacao.md) |

Coberto por testes reais: 200 eventos publicados durante a indisponibilidade resultam em exatamente 200 itens depois do
`unpause` (0 perdas, 0 no DLT); 20 consultas sucessivas com o banco congelado respondem 503 em ~1,3 s cada, nunca 404 nem saldo antigo.

## Observabilidade e operação

**Duas portas, propositalmente:**

| Porta | Conteúdo | Exposição |
|-|-|-|
| **8080** | API (`/balances/{id}`, `/openapi.yaml`) | Pública |
| **8082** | Actuator: `health` e `prometheus` | **Não deve ser roteada pelo balanceador público** (proteção do endpoint é do gateway/rede); o compose local a publica por conveniência |

**Saúde** ([ADR-0013](docs/adr/0013-observabilidade.md)):

| Endpoint (porta 8082) | Significado |
|-|-|
| `/actuator/health/liveness` | Só o estado do processo. Permanece `UP` com o DynamoDB fora. É o `HEALTHCHECK` da imagem |
| `/actuator/health/readiness` | Só a capacidade do próprio processo. **Permanece `UP` com o DynamoDB fora**: a dependência é compartilhada, e derrubar a readiness tiraria todas as instâncias da rotação ao mesmo tempo, impedindo o 503 rápido com `Retry-After` |
| `/actuator/health/dependencies` | Sonda do DynamoDB (`DescribeTable`, cache de 5 s). Vira 503 quando o banco falha; serve a operação e o alerta, não o balanceador |
| `/actuator/health` (raiz) | **Não usar como sonda**: agrega as dependências e fica 503 com o DynamoDB fora, o que retiraria todas as instâncias |

**Métricas** (`/actuator/prometheus`, contrato em [`observability.md`](specs/001-consulta-saldo/contracts/observability.md)):

- `balance_events_total{outcome,reason}`: **exatamente um desfecho por mensagem** (`processed`, `obsolete`, `duplicate`, `rejected`);
  a soma reconcilia com o número de mensagens consumidas.
- Latência com histograma (agregável com `histogram_quantile`): `http_server_requests`, `balance_ingest_duration`,
  `balance_store_read_duration`, `balance_store_write_duration`.
- Sinais de alerta: `balance_dlt_publish_failures_total > 0`, `balance_events_total{reason="unprocessable_event"}`,
  `balance_store_read_corrupted_total`, `balance_consumer_backpressure_total`, `balance_dependency_up{dependency="dynamodb"} == 0`,
  estado do circuit breaker (`resilience4j_circuitbreaker_state{name="dynamodb-read"}`) e lag do consumer.

**Logs** JSON estruturados com `correlationId`, `accountId` e `transactionId` no MDC. **Nunca** saldo, titular, payload nem
mensagem de parser (teste de privacidade com valores sentinela). Tracing distribuído não foi implementado (ver abaixo).

**Imagem** ([ADR-0015](docs/adr/0015-empacotamento-e-operacao.md)): não-root (uid 10001), heap relativa à memória do container
(`MaxRAMPercentage=75`, `ExitOnOutOfMemoryError`), `HEALTHCHECK` na liveness, `ENTRYPOINT` em exec form (a JVM recebe o `SIGTERM`),
tags fixas do Temurin (`21.0.12_8-*-noble`), que exigem rotina periódica de atualização.

## Configuração

Tudo por variáveis de ambiente com defaults para o ambiente local; o compose sobrescreve os hosts. Lista completa e descrição em
[`contracts/configuration.md`](specs/001-consulta-saldo/contracts/configuration.md). As principais:

| Variável | Padrão | Descrição |
|-|-|-|
| `SERVER_PORT` / `MANAGEMENT_SERVER_PORT` | `8080` / `8082` | Portas da API e do Actuator |
| `DYNAMODB_ENDPOINT` | `http://localhost:8000` | Em produção, **vazio** para usar o endpoint AWS e a cadeia padrão de credenciais |
| `DYNAMODB_REGION` / `BALANCE_TABLE_NAME` | `us-east-1` / `AccountBalances` | Região e tabela |
| `DYNAMODB_READ_CONSISTENT` | `true` | Leitura fortemente consistente (custo 2x, ADR-0011) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Brokers |
| `BALANCE_EVENTS_TOPIC` / `BALANCE_EVENTS_DLT_TOPIC` | `transacoes-financeiras-processadas` / `<tópico>.DLT` | Tópicos |
| `KAFKA_CONSUMER_GROUP_ID` / `KAFKA_LISTENER_CONCURRENCY` | `consulta-saldo` / `4` | Grupo e threads (total entre instâncias <= partições) |
| `KAFKA_BACKOFF_INITIAL_MS` / `_MAX_MS` / `_JITTER_MS` | `500` / `30000` / `250` | Backoff da falha transitória |
| `BALANCE_FUTURE_TOLERANCE` | `PT5M` | Tolerância de timestamp futuro |
| `BALANCE_CB_*` | ver contrato | Janela, mínimo de chamadas, taxas e espera do circuit breaker |
| `LOGGING_STRUCTURED_FORMAT_CONSOLE` | `logstash` | Logs JSON |

## Decisões de arquitetura (ADRs)

Cada ADR tem contexto, decisão, alternativas e consequências.

| ADR | Decisão |
|-|-|
| [0001](docs/adr/0001-arquitetura-hexagonal-por-bounded-context.md) | Arquitetura hexagonal por bounded context, verificada por Konsist |
| [0002](docs/adr/0002-modelagem-dynamodb-snapshot-por-conta.md) | Snapshot por conta, `pk=ACCOUNT#id`/`sk=BALANCE`, sem GSI nem ledger |
| [0003](docs/adr/0003-precedencia-deterministica-e-escrita-condicional-atomica.md) | Precedência `(timestamp, txId)` e escrita condicional atômica |
| [0004](docs/adr/0004-classificacao-de-desfechos-duplicado-versus-obsoleto.md) | Duplicado x obsoleto pelo item antigo (`ALL_OLD`) e anomalia |
| [0005](docs/adr/0005-representacao-de-dinheiro-e-tempo.md) | `BigDecimal`/`N` para dinheiro, microssegundos para tempo |
| [0006](docs/adr/0006-validacao-estrita-e-catalogo-de-motivos.md) | Validação estrita e catálogo fechado de motivos |
| [0007](docs/adr/0007-consumer-kafka-at-least-once-e-particionamento.md) | Consumer at-least-once e particionamento |
| [0008](docs/adr/0008-erros-transitorios-permanentes-backpressure-e-dlt.md) | Erros transitórios x permanentes, backpressure e DLT |
| [0009](docs/adr/0009-uma-camada-de-retry-e-clientes-dynamodb-separados.md) | Uma camada de retry e clientes DynamoDB separados |
| [0010](docs/adr/0010-circuit-breaker-na-leitura-com-resilience4j.md) | Circuit breaker na leitura (Resilience4j programático) |
| [0011](docs/adr/0011-leitura-fortemente-consistente.md) | Leitura fortemente consistente |
| [0012](docs/adr/0012-api-problem-details-e-openapi-contract-first.md) | Problem Details e OpenAPI contract-first |
| [0013](docs/adr/0013-observabilidade.md) | Observabilidade: porta separada, saúde em grupos, logs JSON |
| [0014](docs/adr/0014-estrategia-de-testes-e-evidencia-de-corretude.md) | Estratégia de testes e evidência de corretude |
| [0015](docs/adr/0015-empacotamento-e-operacao.md) | Imagem não-root, heap relativa, healthcheck, tags fixas |

## O que NÃO foi feito e por quê

Cada item foi uma decisão consciente (escopo, custo, ausência de requisito), com o desenho proposto para quando o gatilho aparecer
([`research.md`](specs/001-consulta-saldo/research.md) R-17).

| Item | Por que não | Desenho proposto |
|-|-|-|
| Ledger de transações (`TX#` com TTL) | Dobra a escrita e concentra carga na partição da conta; o requisito é o saldo atual e a corretude independe dele | Duas escritas independentes e idempotentes (`PutItem` com `attribute_not_exists` + a `UpdateItem` do snapshot) |
| GSI por titular | Não há padrão de acesso; cada GSI soma WCU a cada evento | `gsi1pk=OWNER#id`, `gsi1sk=ACCOUNT#id`, índice esparso com projeção parcial |
| Write sharding / coalescência por conta | Sem contas quentes na carga de referência | Coalescência no lote de consumo (só o último evento por conta); gatilho: throttling ou muitos `obsolete` |
| Reprocessamento automático do DLT | A spec o define como manual nesta versão | Roteiro manual documentado; futuro `retry` controlado com limite |
| Tracing distribuído (OpenTelemetry) | O `correlationId` cobre a correlação nos logs e o autorizador não propaga `traceparent` | Habilitar o starter, amostragem e exportador OTLP; o MDC passa a receber `traceId`/`spanId` sem mudar o código |
| Prometheus/Grafana/alertas no compose | Não é requisito; as métricas estão expostas | Regras de alerta sugeridas acima |
| IaC (Terraform/CDK), PITR e deletion protection | Fora do escopo de um repositório de avaliação | Descritos em `data-model.md` 4.1 |
| Autenticação/autorização e rate limiting | Tratados no gateway/rede | Gateway na frente; a porta 8082 nunca pública |
| Schema Registry / Avro | O contrato JSON é imposto pelo produtor | n/a |
| Teste de mutação (PIT) e carga sustentada multi-instância | Tempo; sem gate no starter | k6 (opcional, abaixo) |
| Multi-região / global tables | Fora da carga de referência | A chave de precedência é determinística, então converge sem coordenação |

## Riscos conhecidos e o que não foi verificado

- **SC-001 NÃO VERIFICADO.** A meta de latência da consulta (p50 <= 50 ms e p99 <= 300 ms sob 500 req/s) só seria comprovada pelo teste
  de carga k6 **opcional** (tarefas T178-T179), que **não foi executado**. O que existe é a medição de que o 503 sai em milissegundos
  com o circuito aberto e a de ingestão <= 5 s (SC-002) nos testes de integração; latência sustentada sob carga não foi medida.
- **DLT fora do ar**: a nova tentativa de publicar não tem backoff exponencial próprio (limitada por `max.block.ms` + timeout de
  envio, ~3-5 s). Nada se perde; o sinal é `balance_dlt_publish_failures_total`.
- **Falha permanente do armazenamento tratada como transitória** (por exemplo tabela removida): a ingestão fica retida indefinidamente,
  sem perder mensagens; o sinal é `balance_consumer_backpressure_total` e `dependencies` em 503.
- **Custo de eventos obsoletos**: cada evento com condição falsa ainda consome 1 WCU.
- **Anomalia `conflicting_duplicate`** (mesma chave, conteúdo diferente) fica fora da garantia de convergência: vale o primeiro.
- **Tags fixas do Docker** congelam patches; sem a rotina de atualização a base envelhece.
- **`/actuator/health` (raiz)** usado como sonda retiraria todas as instâncias com o DynamoDB fora; use `liveness` e `readiness`.
- **DynamoDB Local** (in-memory) valida a lógica de condição, mas não reproduz throttling, latências nem particionamento reais da AWS.
- A confirmação dos workflows do GitHub Actions depende do push; localmente foram executados os equivalentes (`assemble testClasses`,
  `check`, `make integration-test`, `docker build`).

## Uso de IA

Este projeto foi desenvolvido com apoio de IA (Claude Code), com **uso autorizado pelo Itaú** sob a condição de explicar a
metodologia. Os agentes conduziram o fluxo do GitHub Spec Kit; as decisões de negócio e de arquitetura foram tomadas e aprovadas
pelo autor humano, e toda saída foi verificada por gates automáticos (testes, cobertura, Konsist). Os commits **não** carregam
co-autoria de IA. O processo, os papéis, as correções feitas pela revisão e como auditar estão em
[`docs/metodologia-ia.md`](docs/metodologia-ia.md).

## Stack e imagens Docker

| Categoria | Tecnologia |
|-|-|
| Linguagem / runtime | Kotlin 2.3.21, Java 21 (Eclipse Temurin 21.0.12) |
| Framework | Spring Boot 4.1.0 (Spring Framework 7), Spring MVC, Spring Kafka, Actuator |
| Banco | Amazon DynamoDB (AWS SDK for Java v2 2.46.7, cliente HTTP Apache 5) |
| Mensageria | Kafka (protocolo); broker local Redpanda |
| Resiliência / métricas | Resilience4j 2.4.0 (circuit breaker), Micrometer + Prometheus |
| Testes | JUnit 5, Mockito, Konsist, kotest-property, Awaitility, JaCoCo (gate 90%) |
| Build | Gradle 9.5.1 (Kotlin DSL) |

| Serviço | Imagem | Finalidade |
|-|-|-|
| `app` | build local (`eclipse-temurin:21.0.12_8-jdk-noble` -> `eclipse-temurin:21.0.12_8-jre-noble`) | a aplicação |
| `dynamodb` | `amazon/dynamodb-local:3.3.0` | DynamoDB local (in-memory) |
| `dynamodb-seed` | `amazon/aws-cli:2.36.8` | cria a tabela e grava a conta de exemplo (idempotente) |
| `dynamodb-admin` | `aaronshaf/dynamodb-admin:5.3.4` | console web da tabela |
| `redpanda` | `docker.redpanda.com/redpandadata/redpanda:v26.1.14` | broker Kafka-compatível (single-node) |
| `redpanda-seed` | `docker.redpanda.com/redpandadata/redpanda:v26.1.14` | cria os tópicos (idempotente, sem publicar mensagens) |
| `redpanda-console` | `docker.redpanda.com/redpandadata/console:v3.9.0` | console web de tópicos e mensagens |

Todas as imagens usam versões fixas (nunca `latest`). O `app` só inicia depois que os seeds terminam com sucesso
(`service_completed_successfully`) e sua saúde no compose é a readiness da porta 8082.
