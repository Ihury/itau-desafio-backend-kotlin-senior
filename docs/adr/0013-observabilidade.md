# ADR-0013: Observabilidade: Actuator em porta separada, saúde em grupos e logs JSON

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípio VII; `specs/001-consulta-saldo/research.md` R-13 e R-16; `contracts/observability.md`; FR-031 a FR-035, SC-010 e SC-011

## Contexto

Operar o serviço em produção exige responder, em menos de 1 minuto e só com métricas e verificações de saúde, se ele está vivo, se está
pronto e qual a proporção de mensagens inválidas e obsoletas (SC-011). Exige também que toda mensagem consumida tenha exatamente um
desfecho contabilizado (SC-010) e que os logs identifiquem conta, transação e correlação sem expor dados pessoais nem saldos (FR-032).

Havia uma decisão delicada: onde colocar a saúde do DynamoDB. Numa readiness "clássica" ela derrubaria **todas** as instâncias do
balanceador ao mesmo tempo (a dependência é compartilhada), e o cliente perderia justamente o `503 + Retry-After` rápido que o
circuit breaker devolve (ADR-0010, SC-008). A documentação do Spring Boot desaconselha dependências externas compartilhadas na readiness.

## Decisão

- **Actuator em porta própria** (`MANAGEMENT_SERVER_PORT`, padrão 8082), com só `health`, `info` e `prometheus` expostos. Nada de
  `/actuator/**` responde na porta da API (8080; coberto por teste). A porta de gerenciamento não deve ser roteada pelo balanceador
  público (a proteção do endpoint é do gateway/rede).
- **Saúde em três grupos**: `liveness` = `livenessState`; `readiness` = **somente** `readinessState` (capacidade do próprio processo de
  atender); `dependencies` = `DynamoDbHealthIndicator` (probe `DescribeTable` da tabela, timeout de 500 ms, cache de 5 s, sem detalhes:
  `show-details=never`). Com o DynamoDB fora, `dependencies` fica 503 e `liveness`/`readiness` seguem 200: a instância **não sai de
  rotação**, a API responde 503 explícito e a ingestão faz backpressure. A raiz `/actuator/health` agrega as dependências e por isso
  **não deve ser usada por balanceador ou orquestrador**: as sondas são `/actuator/health/liveness` e `/actuator/health/readiness`.
- **Gauge `balance.dependency.up{dependency="dynamodb"}`** (1/0), avaliado sobre o mesmo estado em cache do indicador: a raspagem do
  Prometheus renova o estado mesmo que ninguém consulte o grupo `dependencies`. É a base do alerta de indisponibilidade sem tirar a
  instância de rotação.
- **Micrometer + Prometheus**: `balance.events{outcome,reason}` com **exatamente um desfecho por mensagem** (`rejected` só é contado
  depois que o DLT confirma a publicação); timers com histograma `balance.ingest.duration{outcome}` (SLO de 5 ms a 2,5 s; medido **por entrega**, não por mensagem: uma mensagem reentregue por falha
  transitória gera uma amostra por tentativa, com `outcome=error` nas que falham),
  `balance.store.write.duration{result}` e `balance.store.read.duration{result}` (5 ms a 2 s), todos com as séries iniciadas em zero;
  `http.server.requests` com SLO de 50 ms a 2 s; métricas do circuit breaker e do listener Kafka (`spring.kafka.listener.observation-enabled`).
  p50/p99 saem de `histogram_quantile` sobre os buckets (agregável entre instâncias), nunca de percentis client-side.
- **Logs JSON nativos do Spring Boot** (`logging.structured.format.console=logstash`) com o MDC como chaves de topo: `correlationId`
  (HTTP: `X-Correlation-Id` validado ou gerado; Kafka: `<topic>-<partition>@<offset>`), `accountId` e `transactionId` (só depois do
  parse). Nunca são logados saldo, titular, payload nem mensagem de exceção de parser: a rejeição loga só o motivo e as coordenadas.
  Um teste percorre todos os caminhos com valores sentinela, e o `ArchitectureTest` proíbe `catch` que engula exceção sem log, métrica ou
  `throw`. Com o circuit breaker da leitura aberto, cada rejeição loga em DEBUG (sem pilha) e o WARN sai só na transição de estado
  do breaker e nas falhas reais de leitura (ADR-0010); falha de configuração ou credencial do DynamoDB (`cause=misconfigured`) loga
  em ERROR com a classe da exceção do SDK, o `errorCode` e o `statusCode`, nunca a mensagem livre. O banner do Spring é desligado para não quebrar o parse do coletor (uma linha, um objeto JSON).
- **Encerramento gracioso**: `server.shutdown=graceful`, 30 s por fase, `immediate-stop` no consumer e commit só pelo container: o que
  não foi persistido não é confirmado e é reentregue (ADR-0007), reconciliado pela idempotência.
- **Tracing distribuído (OpenTelemetry) fica como evolução** (R-16): o `correlationId` cobre a correlação nos logs e o autorizador
  externo não propaga `traceparent`. O caminho é habilitar o starter, `management.tracing.sampling.probability` e um exportador OTLP;
  o MDC passa a receber `traceId`/`spanId` sem mudar o código.

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| DynamoDB na readiness (proposta original) | Todas as instâncias saem de rotação juntas: o cliente perde o 503 rápido com `Retry-After` e o circuit breaker/SC-008 perdem o sentido |
| Sem sinal da dependência | O operador ficaria cego ao estado do banco; o grupo `dependencies` e o gauge dão o alerta sem afetar a rotação |
| Actuator na porta 8080 | Amplia a superfície pública e mistura métricas com a API; a porta separada dispensa autenticação no endpoint |
| Percentis client-side (`publishPercentiles`) | Não são agregáveis entre instâncias; histograma com SLO permite `histogram_quantile` |
| Probe por `GetItem` | Consumiria RCU a cada probe; `DescribeTable` é barato e prova tabela e credenciais |
| Probe sem cache | Cada raspagem ou consulta de saúde iria ao banco; o cache de 5 s limita a carga e mantém gauge e grupo coerentes |
| Log em texto plano | Não é parseável por coletor; o JSON nativo do Boot dispensa dependência de encoder |
| OpenTelemetry agora | Exige coletor OTLP no compose, amostragem e CPU, sem propagação de contexto pelo autorizador |

## Consequências

- (+) Alertas e painéis reconciliam por desfecho (`sum(balance_events_total)` = mensagens consumidas) e por latência (p50/p99 da API,
  da escrita e da ingestão).
- (+) A instância continua em rotação com o DynamoDB fora e a operação enxerga a falha por `dependencies` e pelo gauge.
- (+) Os logs são seguros por construção e verificados por teste, inclusive as mensagens do Spring Kafka para o retry.
- (-) `/actuator/health` (raiz) fica 503 com o DynamoDB fora: quem apontar um balanceador para ele retira todas as instâncias.
  Mitigação: documentado aqui, no `application.yaml` e no README.
- (-) O estado de `dependencies` pode estar até 5 s defasado (cache do probe).
- (-) O gauge pode disparar o probe (até 500 ms) dentro da raspagem quando o cache expirou e o DynamoDB está lento ou fora.
- (-) Sem tracing distribuído na v1: a correlação é por `correlationId` nos logs.
