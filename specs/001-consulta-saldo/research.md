# Research: Consulta de Saldo

**Feature**: `001-consulta-saldo` | **Spec**: [spec.md](./spec.md) | **Plano**: [plan.md](./plan.md) | **Data**: 2026-09-29

Resolve todas as incógnitas do Technical Context. Formato de cada decisão: **Decision / Rationale / Alternatives considered**.
As decisões que dependiam de escolha do usuário foram reunidas na seção 5, que registra o que foi decidido e por quem
(o texto original as marcava como "decisão proposta — requer validação", antes da revisão do plano).

## 0. Método e evidências

- Versões e compatibilidade consultadas em fontes reais (Maven Central `maven-metadata.xml`/POMs, fontes do Spring
  Framework no GitHub, Docker Hub) em 2026-09-29; nada foi assumido da memória.
- **Spikes executados** (projeto descartável fora do repositório, Boot 4.1.0 + Kotlin 2.3.21 + Java 21, contra
  **DynamoDB Local 3.3.0** e **Redpanda v26.1.14** reais, as mesmas imagens do `docker-compose.yml`). O que cada spike
  provou está citado como *[Spike]* nas decisões. Não são versionados; a implementação reproduz cada um como teste (TDD).
- Baseline verificado do starter (`./gradlew dependencies`): Spring Boot 4.1.0 -> Spring Framework 7.0.8, Spring Kafka 4.1.0,
  kafka-clients 4.2.1, Jackson 3.1.4, Micrometer 1.17.0, JUnit Jupiter 6.0.3, Mockito 5.23.0, Tomcat 11.0.22, AWS SDK 2.46.7
  (cliente HTTP síncrono padrão: `apache5-client`), Konsist 0.17.3, JaCoCo 0.8.12.

## 1. Dependências novas: versão e evidência de compatibilidade

| Dependência | Escopo | Versão | Evidência | Status |
|-------------|--------|--------|-----------|--------|
| `io.github.resilience4j:resilience4j-circuitbreaker` | main | **2.4.0** | Última release (`maven-metadata.xml`, 2026-03-14). Só depende de `resilience4j-core` e SLF4J (sem Spring). *[Spike]* rodou sob Boot 4.1.0: abriu o circuito e lançou `CallNotPermittedException` | Compatível (biblioteca pura, uso programático) |
| `io.github.resilience4j:resilience4j-micrometer` | main | **2.4.0** | POM compila contra `micrometer-core 1.16.0`; o Boot 4.1.0 gerencia `1.17.0`. *[Spike]* `TaggedCircuitBreakerMetrics.bindTo(MeterRegistry)` exportou `resilience4j_circuitbreaker_state{name="dynamodb-read",state="open"} 1.0` em `/actuator/prometheus` | Compatível na prática; **risco baixo** (compilada com 1.16): coberto por teste de integração de métricas. Traz módulos `bulkhead/retry/ratelimiter/timelimiter` como dependências de runtime não usadas (aceito) |
| `io.kotest:kotest-property` | test | **6.2.5** | Última release (2026-09-10). *[Spike]* `checkAll(200, Arb.list(Arb.int()))` rodou dentro de teste JUnit Jupiter 6.0.3 com Kotlin 2.3.21 e `kotlinx-coroutines-core` 1.10.2 (já transitiva). Biblioteca sem *engine*: não adiciona motor de teste | Compatível |
| `org.springframework.boot:spring-boot-starter-actuator` | main | **4.1.0** (BOM) | Traz `spring-boot-starter-micrometer-metrics` -> `micrometer-core 1.17.0`; `HealthIndicator` está em `org.springframework.boot.health.contributor` (Boot 4). *[Spike]* liveness/readiness `UP` | Compatível |
| `io.micrometer:micrometer-registry-prometheus` | main | **1.17.0** (BOM) | `micrometer.version=1.17.0` e `prometheus-client 1.5.1` no `spring-boot-dependencies-4.1.0.pom`. *[Spike]* `/actuator/prometheus` 200 | Compatível |
| `org.awaitility:awaitility-kotlin` | test/integrationTest | **4.3.0** (BOM) | `awaitility.version=4.3.0` no BOM do Boot | Compatível |
| `tools.jackson.*` (parser estrito) | main | 3.1.4 (já presente) | *[Spike]* `readTree` + `USE_BIG_DECIMAL_FOR_FLOATS` + `STRICT_DUPLICATE_DETECTION` | Sem dependência nova |

Avaliadas e **não adotadas**: `springdoc-openapi-starter-webmvc-api` 3.1.1 (R-12), `jqwik` 1.10.1 (R-14),
`resilience4j-spring-boot4` 2.4.0 (R-11), Testcontainers 2.0.5 (R-14), `spring-boot-starter-opentelemetry` 4.1.0 (R-16),
`spring-boot-starter-validation` (o `accountId` é validado no domínio; sem Bean Validation).

Sem alteração de versões existentes: AWS SDK BOM 2.46.7 (última: 2.55.7; sem requisito que justifique o bump), JaCoCo 0.8.12
(última: 0.8.15; o gate de 90% roda com a atual), Konsist 0.17.3 (é a última).

---

## 2. Decisões

### R-01. Bounded context `balance`, remoção do exemplo `hello`, Konsist generalizado

- **Decision**: novo pacote `br.com.itau.challenge.balance` com `domain/{model,exception}`, `port/{input,output}`, `application`,
  `adapter/{input/web,input/kafka,output/dynamodb,output/metrics}` e `config` (composition root, fora das quatro camadas).
  **Remover** o exemplo `hello` (fontes, testes, seed `GreetingMessages`, `http/hello.http`) — *decisão aprovada pelo autor na revisão do plano*.
  Generalizar `HexagonalArchitectureTest` para **todos** os pacotes de negócio (`Konsist.scopeFromProduction()`, camadas por
  padrão `..domain..` etc.) e acrescentar: (a) `domain` importa só `kotlin.*`/`java.*`/o próprio domínio (nenhum Spring, AWS,
  Kafka, Jackson, Micrometer, Resilience4j); (b) `application` importa só domain/port, `org.springframework.stereotype.Service`
  e `org.slf4j`; (c) cada tecnologia de adapter (`input.web`, `input.kafka`, `output.dynamodb`, `output.metrics`) não depende
  das demais; (d) ninguém depende de `config`; (e) *guarda*: todo subpacote direto de `br.com.itau.challenge` é um contexto com
  exatamente `domain/port/application/adapter` (+`config`) — um contexto novo não escapa da verificação.
- **Rationale**: Constitution I exige cobertura de *todos* os contextos; manter um endpoint `/hello` e um `@KafkaListener` de
  saudações num serviço de core banking é ruído e risco (consumidor de tópico alheio, tabela sem uso). O starter usa `@Service` na
  camada `application`; mantemos essa convenção, limitada por regra Konsist ao único stereotype necessário. Métricas e relógio
  entram por *port* (`ProcessingMetrics`) e `java.time.Clock`, então a camada `application` continua livre de Micrometer.
- **Alternatives**: manter `hello` ao lado (contexto extra a proteger, confunde o avaliador); `application` 100% sem Spring com
  `@Bean` no `config` (mais puro, mas diverge do starter sem ganho verificável); ArchUnit (mais uma dependência; Konsist já é padrão do starter).

### R-02. Estrutura de portas

- **Decision**: portas de entrada `ProcessTransactionEventUseCase` (comando = `TransactionEvent`, retorna `ApplyResult`) e
  `GetBalanceUseCase` (retorna `BalanceSnapshot`; lança `AccountNotFoundException`/`AccountDisabledException`). Portas de saída
  separadas por papel: `BalanceSnapshotWriter.applyIfNewer(snapshot): ApplyResult` e `BalanceSnapshotReader.find(id): BalanceSnapshot?`
  (ISP; permite dois clientes DynamoDB com políticas distintas, R-10) e `ProcessingMetrics`.
  `BalanceStoreUnavailableException` (transitória) e `BalanceStoreRejectedException` (permanente) são exceções de domínio: a
  camada de aplicação/consumer classifica sem conhecer o SDK.
- **Rationale**: o consumer e a API têm perfis de falha opostos (backpressure x falhar rápido); portas separadas tornam isso explícito.
- **Alternatives**: uma porta `BalanceRepository` única (mistura leitura e escrita, força um cliente/política único).

### R-03. Modelagem DynamoDB (critério do cliente: PK, SK, índices)

- **Decision**: tabela `AccountBalances`, **PK `pk` = `ACCOUNT#<accountId minúsculo>`, SK `sk` = `BALANCE`** (snapshot vigente),
  um item por conta, on-demand. **Sem GSI. Sem ledger `TX#...` na v1.** Layout completo em `data-model.md` seção 4.
  GSI por titular e ledger com TTL ficam documentados como evolução com desenho e gatilho (R-17).
- **Rationale**:
  - *Padrões de acesso*: só AP1 (leitura por conta) e AP2 (escrita por conta); ambos por chave primária -> nenhum índice tem
    padrão de acesso (Constitution VIII). Um GSI custaria WCU adicional em toda escrita.
  - *SK constante*: o *key schema* é imutável; `pk/sk` genéricos com prefixo permitem adicionar `TX#`/outros itens à mesma partição
    sem migração. Custo zero hoje.
  - *Ledger avaliado e recusado*: (a) **custo**: dobraria as escritas (~2 WCU/evento; ≈ +1.000 WCU/s na carga de referência) para
    entregar um histórico que a spec explicitamente não expõe; (b) **hot partition**: itens `TX#` compartilham a partição da conta;
    contas quentes concentrariam o dobro de escrita na mesma chave; (c) **corretude**: a convergência depende só da condição no
    snapshot; o ledger não a melhora; (d) **valor**: só afinaria o rótulo `duplicado x obsoleto` de reentregas *já superadas* (R-04),
    que a spec aceita como "duplicado ou obsoleto". Se um dia for incluído: duas escritas **independentes e idempotentes**
    (`PutItem` do `TX#` com `attribute_not_exists(sk)`, e o `UpdateItem` do snapshot); **nunca `TransactWriteItems`**, pois a
    rejeição do snapshot por obsolescência abortaria a transação inteira e perderia o registro do ledger.
  - *`BatchWriteItem`* não suporta `ConditionExpression`: inaplicável à escrita de snapshot.
- **Alternatives**: `PK=accountId` sem prefixo/SK (mais simples, mas fecha a porta para múltiplos tipos de item); tabela com item
  por transação e "último" por query (`ScanIndexForward=false`, `Limit=1`): leitura mais cara, escrita sem condição = sem
  arbitragem atômica (proibido pela Constitution II).

### R-04. Precedência, escrita condicional e "duplicado x obsoleto" (critério: concorrência)

- **Decision**: precedência `(timestamp µs, transactionId canônico minúsculo)`; **uma** `UpdateItem` condicional
  (`attribute_not_exists(pk) OR lastTxTsMicros < :ts OR (lastTxTsMicros = :ts AND lastTxId < :tx)`) com
  `ReturnValuesOnConditionCheckFailure=ALL_OLD`; nenhum read-modify-write; nenhum lock local.
  **Resolução do item adiado da spec ("duplicado x obsoleto")**: `duplicate` = evento cujo `(timestamp, transactionId)` é
  **igual** ao do snapshot vigente; `obsolete` = qualquer evento de precedência inferior, **incluindo a reentrega de uma
  transação que já foi superada** (sem ledger é indistinguível de qualquer evento antigo; ambos são "sem efeito e contabilizados",
  que é o que FR-005/FR-006 exigem). Mesmo `transaction.id` + mesmo timestamp com conteúdo divergente => `duplicate` +
  anomalia (`balance.events.anomalies{type=conflicting_duplicate}` e log WARN), comparando o item retornado por `ALL_OLD`.
- **Rationale**: o banco arbitra atomicamente entre threads e instâncias; nenhuma leitura extra para classificar.
  *[Spike]* (DynamoDB Local 3.3.0, SDK 2.46.7): `ConditionalCheckFailedException.item()` vem preenchido; 400 escritas concorrentes
  (32 threads, 100 duplicatas, desordem) convergiram exatamente para `max(ts, txId)`; 2.000 UUIDs aleatórios: 0 divergências entre
  a condição do banco e `String.compareTo`. UUID em **minúsculas** é obrigatório (maiúsculas ordenam antes) e `java.util.UUID.compareTo`
  **não** serve (compara `long`s com sinal).
- **Alternatives**: `TransactWriteItems` (ver R-03); versão otimista `version = version + 1` com retry (read-modify-write; proibido);
  ordenar por horário de processamento (proibido, FR-003); `timestamp` como `S` (comparação lexicográfica exigiria zero-padding).

### R-05. Consistência de leitura

- **Decision**: `GetItem` com `ConsistentRead=true` (configurável por `DYNAMODB_READ_CONSISTENT`).
- **Rationale**: FR-027 exige o snapshot "mais recente já efetivado"; leitura eventual poderia devolver o estado anterior logo após a
  escrita (janela de ms a ~1 s) e contradiria a consulta imediata a um evento recém-consumido. Custo: 1 RCU (vs 0,5) por leitura
  (≈ 500 RCU/s na carga de referência) e indisponibilidade em partição de rede — que vira 503, comportamento desejado (FR-030).
- **Alternatives**: leitura eventual (metade do custo; aceitável se o cliente aceitar staleness de ~1 s, então a flag existe);
  cache local com TTL (viola "saldo desatualizado sem sinalização"; YAGNI).

### R-06. Representação de dados, timestamps e fuso

- **Decision**:
  - Dinheiro `BigDecimal` ponta a ponta, **nunca** `Double`/`Float`. No DynamoDB `balanceAmount` é **`N`**, escrito a partir de
    `BigDecimal.toPlainString()` e lido com `BigDecimal(String)` (decisão do usuário). O DynamoDB normaliza zeros à direita
    (`183.10` -> `183.1`): isso **não é perda** (valor exato; JSON Number não carrega escala — RFC 8259 —, e a origem também é JSON Number,
    então a escala recebida é artefato do serializador, não informação de negócio).
  - **Apresentação na API**: a escala é completada até as casas da moeda **sem nunca arredondar**:
    `amount.setScale(max(amount.scale(), Currency.getInstance(code).defaultFractionDigits))` quando `defaultFractionDigits >= 0`
    (moedas com `-1`, ex. XAU, não são ajustadas). Ex.: BRL `183.1` -> `183.10`; `100` -> `100.00`; `10.123` -> `10.123`; JPY `500` -> `500`.
    Serializado com `WRITE_BIGDECIMAL_AS_PLAIN`. A regra vive em `Money` (domínio, só `java.util.Currency`); o DTO apenas a usa.
  - **Limites de domínio (só o necessário)**: precisão <= **38** dígitos significativos (limite do `N`; acima -> `invalid_value`); escala
    negativa (`1E+3`) é expandida a escala 0 somente se `precisão - escala <= 38` (nunca materializa `1E999999999`); escala positiva <= **38**.
    Justificativa do 38: mantém o valor dentro da faixa do `N` (expoente até 1E-130) e limita o tamanho da
    string plana; mais de 38 casas decimais não é valor monetário. Nenhum outro teto (ex.: por moeda) é imposto.
  - Timestamps em µs como `Long`/`N`; `Instant` ao expor. `updated_at` = `ISO_OFFSET_DATE_TIME` no fuso `America/Sao_Paulo`
    (`BALANCE_DISPLAY_ZONE`); frações só quando não nulas (`.433`, `.433123`).
  - Intervalo plausível, **duas regras distintas** (revisão do plano): `transaction.timestamp` >= `2000-01-01T00:00:00Z` (µs; detecta
    unidade s/ms e participa da precedência); `account.created_at` = inteiro em µs (negativo para instantes anteriores a 1970) >= `1900-01-01T00:00:00Z`
    (`BALANCE_MIN_ACCOUNT_CREATED_AT`; contas abertas antes de 2000 são legítimas e **não** podem ir ao DLT; o campo não participa da
    precedência). Limite superior de ambos = relógio do serviço + **tolerância de futuro `PT5M`** (`BALANCE_FUTURE_TOLERANCE`), só para
    validação (FR-012).
  - Jackson: parser do Kafka próprio e estrito (árvore, `USE_BIG_DECIMAL_FOR_FLOATS`, `STRICT_DUPLICATE_DETECTION`, limites de
    payload/aninhamento/tamanho de número); API com `spring.jackson.write.WRITE_BIGDECIMAL_AS_PLAIN=true`.
- **Rationale**: *[Spike]* `N` normaliza `183.10` -> `183.1` (comportamento **esperado e coberto por teste**: round-trip + formatação da
  resposta) e rejeita > 38 dígitos com `DynamoDbException` 400 (falha permanente do armazenamento por dado que o domínio deveria ter
  barrado; por isso o domínio impõe o mesmo teto). O valor numérico é exato em todo o caminho; a escala mínima da moeda é *apresentação*
  (completar com zeros nunca altera o valor). `N` mantém o saldo numérico no banco (comparável/agregável no futuro). *[Spike]* sem `WRITE_BIGDECIMAL_AS_PLAIN` o Jackson 3
  serializa `1E+3`/`1E-7`; com ele `1000`/`0.0000001`/`183.10`. O timestamp `1751641364589998.0` entra como `DecimalNode` e `"97.07"` como
  `StringNode`, permitindo rejeitar coerções silenciosas (um DTO tipado com `ACCEPT_FLOAT_AS_INT` truncaria `1.5` para `1`).
  O limite inferior de 2000 detecta valores em segundos/milissegundos (1,7e9/1,7e12 µs = 1970); nanossegundos (1,7e18) caem no limite
  de futuro. 5 minutos absorvem desvio de relógio normal sem "envenenar" a conta (spec Assumptions). Exemplo do cliente
  (`2025-07-05T18:04:13.433-03:00`) reproduzido byte a byte pelo formatter padrão (*[Spike]*).
- **Alternatives**:
  - **`S` texto plano** (recusado): preservaria uma escala que o contrato JSON não garante (RFC 8259) e que não é informação de negócio;
    deixaria o dinheiro "stringly typed" e não numérico no banco (sem comparação/agregação server-side), e exigiria validar/normalizar texto.
  - **`N` em centavos/unidade mínima** (recusado): é o padrão quando se é dono do contrato; aqui o contrato é decimal + ISO 4217, o que
    exigiria conversão nos dois sentidos, tabela de casas decimais por moeda (0/2/3/-1) e rejeitar ou arredondar frações de centavo
    (arredondar é proibido; rejeitar descartaria eventos válidos da origem).
  - **Fuso/formato de `updated_at` (alternativas recusadas)**: offset fixo `-03:00` (correto hoje, errado para instantes históricos com horário de verão; `ZoneId` é exato);
  sempre 6 dígitos de fração (não reproduz o exemplo do cliente); DTO Jackson tipado com `BigDecimal` (coerção silenciosa e
  erros sem categoria).

### R-07. Consumer Kafka: entrega, deserialização, partições

- **Decision**: `@KafkaListener` **por registro**, `ConsumerRecord<ByteArray?, ByteArray?>`; `ByteArrayDeserializer` para chave e valor;
  `enable.auto.commit=false` explícito; `AckMode=BATCH` (commit após o poll inteiro ser processado); `CooperativeStickyAssignor`;
  `max.poll.records=100`; `max.poll.interval.ms=300000` (default) com invariante testada
  `max.poll.records x apiCallTimeout(escrita)` = 200 s < 300 s; `concurrency=4` por instância (soma entre instâncias <= partições);
  `immediate-stop=true` + `server.shutdown=graceful` + `spring.lifecycle.timeout-per-shutdown-phase=30s` (o container para após o
  registro atual; o resto do poll é reentregue e é idempotente); `stop_grace_period: 40s` no compose. **Partições: 12 (principal) e 3 (DLT)**,
  RF 1 local / 3 produção, DLT com retenção de 14 d — *decisão aprovada pelo autor na revisão do plano*.
- **Rationale**: *[Spike]* `enable.auto.commit=false` e `CooperativeStickyAssignor` confirmados na configuração efetiva do consumer;
  após DLT e recuperação o *lag* do grupo foi 0 (offsets confirmados). Bytes verbatim garantem que o DLT preserve o conteúdo original
  mesmo se binário/UTF-8 inválido (com `StringDeserializer` os bytes seriam substituídos por `U+FFFD`, e `ErrorHandlingDeserializer`
  só entregaria um `DeserializationException` genérico). Como `ByteArrayDeserializer` nunca lança, `ErrorHandlingDeserializer` é
  desnecessário — **divergência da diretriz, justificada**: a categorização fina (7 motivos) exige o parser próprio de qualquer forma
  (R-06). 12 partições: 1.000 ev/s ÷ ~150 ev/s por thread (escrita ~5-7 ms) ≈ 7 consumidores; 12 dá folga sem custo de coordenação
  excessivo e é seguro aumentar porque não há dependência de ordem. DLT com poucas mensagens: 3 partições.
  **Armadilha validada**: o recoverer padrão publica no DLT *na mesma partição* (falharia com 12 -> 3); usa-se destino
  `TopicPartition(dlt, -1)` (particionador decide).
- **Alternatives**: `RECORD` ack (um commit por mensagem: caro a 1.000/s); `MANUAL` (complexidade sem benefício aqui);
  `ErrorHandlingDeserializer(JsonDeserializer)` (perde motivos finos e bytes originais); concorrência = partições (mais threads
  que a instância precisa; configurável); Kafka group protocol KIP-848 (fora do escopo).

### R-08. Classificação de erros, backpressure e DLT (critério: resiliência)

- **Decision**: `DefaultErrorHandler` com `DeadLetterPublishingRecoverer` e três classes:

| Classe | Exceção | Tratamento |
|--------|---------|-----------|
| **Permanente** | `InvalidEventException(reason, detail?)` (formato, campo, id, valor, moeda, timestamp, domínio, inclusive futuro) | *não-retentável* -> DLT imediato com `x-rejection-reason`/`-detail`/`-at`; desfecho `rejected{reason}` |
| **Transitória** | `BalanceStoreUnavailableException(failureCause = THROTTLED\|UNAVAILABLE\|TIMEOUT\|MISCONFIGURED)` (throttling, 5xx, timeout, conexão, DynamoDB fora; `MISCONFIGURED` = tabela inexistente, acesso negado ou credencial ausente/inválida/expirada, só muda o diagnóstico) | `ExponentialBackOff(500 ms, x2, máx 30 s, jitter 250 ms, tentativas ilimitadas)` + `ContainerPausingBackOffHandler` (pausa o container PAI, isto é, todas as threads de consumo da instância; mantém o poll; mensagem fica no broker); **nunca** DLT; `balance.consumer.backpressure` |
| **Não classificada** | qualquer outra exceção (inclui `BalanceStoreRejectedException`) | `FixedBackOff(100 ms, 2)` (3 entregas) e DLT `unprocessable_event` — evita que um defeito determinístico bloqueie a partição para sempre (Constitution III) |

  DLT indisponível => a publicação falha, o registro **não é confirmado** e é reentregue (`waitForSendResultTimeout=5 s`,
  `max.block.ms=3 s`, `acks=all`, idempotência). O desfecho `rejected` é contado no `RetryListener.recovered`.
  Headers de exceção do Spring são excluídos (podem vazar trechos do payload).
- **Rationale**: *[Spike]* (Redpanda real): (1) mensagem-veneno binária no meio de válidas foi ao DLT (partição 0..2 de 3) com header
  customizado, vizinhas processadas, offset confirmado; (2) falha transitória por 8 s: 7 tentativas com intervalos crescentes,
  **0 mensagens no DLT**, retomada ~1 s após a correção e mensagens processadas (não perdidas); (3) DLT inexistente: o registro
  inválido **não foi confirmado**, a partição ficou bloqueada sem perda e drenou ~0,3 s após a criação do tópico; (4) exceção não
  classificada: 3 entregas e DLT, e a mensagem seguinte foi processada. Falhas de validação do parser jamais são tratadas como
  transitórias, e uma falha transitória jamais isola mensagem válida (FR-017). `ExponentialBackOff.setJitter` é nativo do Spring
  Framework 7.0 ("entre `interval - jitter` e `interval + jitter`, escala com o multiplicador") — fonte:
  `spring-core/.../ExponentialBackOff.java` v7.0.8.
- **Limite conhecido (risco R2 na seção 6)**: enquanto o DLT está fora, o *retry da publicação* não tem backoff exponencial próprio
  (cada tentativa é limitada por `max.block.ms`+`waitForSendResultTimeout`, ~3-5 s); alertar por `balance.dlt.publish.failures`.
- **Alternatives**: `@RetryableTopic` (tópicos de retry: mais tópicos e reordenação, e o broker já é o buffer); retry em memória
  bloqueante (estoura `max.poll.interval.ms` e desperdiça threads); pausa manual com `Consumer.pause` (reimplementaria o handler nativo).

### R-09. Record x batch, coalescência e paralelismo

- **Decision**: listener por registro; **sem coalescência na v1**; paralelismo por `concurrency` do container (padrão 4, <= partições),
  sem virtual threads.
- **Rationale**: o gerador do starter e o autorizador publicam **sem chave e com conta aleatória**: na prática dois eventos da mesma
  conta raramente caem no mesmo lote, então a coalescência economizaria pouco enquanto complica (classificação de erro parcial com
  `BatchListenerFailedException`; um evento coalescido precisa virar `obsolete` para reconciliar SC-010). Baseline local *[Spike]*:
  1.330 updates/s com 1 thread e 3.700/s com 4 threads no DynamoDB Local; em AWS ~5-10 ms/escrita => ~150 ev/s por thread e ~600/s por
  instância com 4 threads (2 instâncias atendem 1.000 ev/s). Virtual threads não ajudam: o gargalo é I/O de baixo volume por
  thread, o pool de conexões já limita o paralelismo, e o cliente HTTP usa I/O bloqueante síncrono.
  Gatilho para reavaliar: razão `obsolete` alta ou throttling em contas quentes -> coalescência por conta no lote.
- **Alternatives**: `BatchListener` + agrupar por conta + `UpdateItem` em paralelo (mais throughput por poll; complexidade não
  justificada pela carga de referência); `BatchWriteItem` (sem condição: inaplicável).

### R-10. Retry, timeouts e clientes DynamoDB (critério: resiliência; Constitution V)

- **Decision**: **exatamente uma camada de retry por chamada remota**, dois `DynamoDbClient` com pools e políticas próprios:

| | Cliente de **escrita** (consumer) | Cliente de **leitura** (API) |
|---|---|---|
| Camada de retry | **Error handler do consumer** (R-08). SDK: `AwsRetryStrategy.doNotRetry()` (1 tentativa) | **SDK `standard`**, `maxAttempts=2` (1 retry, backoff exponencial com jitter nativo, *token bucket* de cota) |
| `apiCallAttemptTimeout` / `apiCallTimeout` | 2 s / 2 s | 0,6 s / 1,5 s (SC-008: 503 em <= 2 s) |
| Conexão / aquisição do pool | 0,3 s / 0,3 s | 0,3 s / 0,3 s |
| Pool (`maxConnections`) | 50 | 100 (falha rápida ao esgotar = bulkhead) |
| Circuit breaker | não (backpressure é o mecanismo) | sim (R-11) |

- **Rationale**: *[Spike]* o cliente DynamoDB padrão usa `maxAttempts=9`, backoff base 25 ms e *throttling backoff* 500 ms: somar isso
  ao backoff do consumer geraria empilhamento de retentativas e tempestades. Na escrita, o consumer já *precisa* de
  redelivery+pausa para indisponibilidades longas; uma segunda camada no SDK só multiplicaria tentativas. Na leitura não há redelivery,
  então o SDK é a única camada, curta para falhar rápido. `RequestOverrideConfiguration` permite sobrescrever timeouts por requisição
  mas **não** a estratégia de retry (verificado em `sdk-core` 2.46.7), logo dois clientes; isso também isola os pools: rajada de
  ingestão não pode esgotar as conexões da API. *[Spike]* recusa de conexão falha em ~2 ms; buraco negro em ~500 ms (timeout de
  conexão) como `SdkClientException`.
  Modo `adaptive` avaliado e recusado: o limitador de taxa do cliente introduz espera antes da requisição (contradiz falhar rápido).
- **Alternatives**: SDK com 3 tentativas + backoff do consumer (duas camadas: proibido); um cliente único com timeouts por requisição
  (não isola pool nem permite retry distinto); `@Retryable` do Spring 7 (segunda camada; ver R-11).

### R-11. Circuit breaker: Resilience4j x recursos nativos do Spring Framework 7 (critério: resiliência)

- **Decision**: **Resilience4j 2.4.0, módulo core `resilience4j-circuitbreaker` + `resilience4j-micrometer`, uso programático** num decorator
  do port de leitura (`CircuitBreakingBalanceSnapshotReader`, em `adapter/output/dynamodb`). Config (env em `contracts/configuration.md`):
  janela por tempo 10 s, mínimo 20 chamadas, falha >= 50% ou lentas (>0,5 s) >= 80% abrem; espera OPEN 10 s (automática para HALF_OPEN);
  5 chamadas em HALF_OPEN; conta como falha somente `BalanceStoreUnavailableException`. `CallNotPermittedException` é convertida na mesma
  exceção de domínio -> **503 + `Retry-After: 10`**. Escrita/consumer sem circuit breaker (backpressure já faz esse papel).
- **Rationale**: *Spring Framework 7.0.8 tem* `org.springframework.core.retry` (`RetryTemplate`) e, em `spring-context`,
  `@Retryable`/`@ConcurrencyLimit`/`@EnableResilientMethods` — verificado nos JARs — **mas não tem circuit breaker**, e `@Retryable`
  criaria uma segunda camada de retry (R-10). `@ConcurrencyLimit` bloqueia (não falha rápido); o pool com timeout de aquisição de 0,3 s
  já cumpre o papel de bulkhead. `resilience4j-spring-boot4` 2.4.0 existe, mas é compilado contra Boot 4.0.0/Spring 7.0.2 (não 4.1.0),
  traz auto-configuração/AOP com "mágica" (anotações e propriedades) e não é necessário: o core é biblioteca pura, sem acoplamento a versão
  do Spring. *[Spike]* sob Boot 4.1.0: CB abriu após 4 falhas, chamadas seguintes lançaram `CallNotPermittedException`, e o estado
  apareceu no Prometheus.
- **Alternatives**: circuit breaker próprio (~80 linhas; reimplementa janela deslizante, half-open e métricas, menos defensável);
  `resilience4j-spring-boot4` (ver acima; risco de compatibilidade não comprovado com 4.1); Spring Cloud CircuitBreaker (abstração
  desnecessária); sem circuit breaker (o critério do cliente pede "onde oportuno" e a Constitution V o exige na API).

### R-12. API HTTP e contrato OpenAPI

- **Decision**: `GET /balances/{accountId}`; o controller recebe `String`, chama `AccountId.parse` (regex estrita; `UUID.fromString`
  aceitaria `1-1-1-1-1`) **antes** de tocar o armazenamento -> 400. Mapeamento: 200 / 400 `requisicao-invalida` / 404
  `conta-nao-encontrada` / 409 `conta-desabilitada` / 503 `servico-indisponivel` (+`Retry-After`) / 500 `erro-interno`, todos
  `application/problem+json` (RFC 9457), `type` = `urn:problem-type:consulta-saldo:<slug>` (estável, sem depender de domínio real),
  via `@RestControllerAdvice` (`ResponseEntityExceptionHandler`) e `spring.mvc.problemdetails.enabled=true` para erros do framework
  (404/405). `Cache-Control: no-store`; `X-Correlation-Id` aceito (validado `[A-Za-z0-9._-]{1,64}`) ou gerado e devolvido.
  **Contrato OpenAPI 3.1 estático, contract-first** (`contracts/openapi.yaml`), servido em `/openapi.yaml`; um teste parseia o arquivo
  (SnakeYAML já transitivo — confirmar escopo de compilação na implementação) e garante que status, `type`s e campos do documento
  coincidem com as respostas reais do MockMvc (proteção contra drift). **Sem springdoc.**
- **Rationale**: o documento já foi validado (`openapi-spec-validator` 3.1 + exemplos contra os schemas, Phase 1). springdoc **3.1.1**
  é compatível (parent Boot 4.1.0; *[Spike]* `/v3/api-docs` retornou `openapi: 3.1.0` no Boot 4.1.0), mas gera o documento do código
  (anotações espalhadas nos controllers), embute `swagger-ui` (superfície extra) e o CHANGELOG 3.1.1 (2026-09-06) lista **oito avisos de
  segurança** (MCP, cache sem limite por locale, XSS do DOMPurify do swagger-ui): para um serviço de core banking sem requisito de UI, é
  superfície de ataque sem contrapartida. Erros nunca expõem pilha, saldo, titular ou nomes de infraestrutura (FR-026).
- **Alternatives**: springdoc (acima); serviço só com o arquivo em `docs/` (não verificável em runtime); `Retry-After` dinâmico
  (o `resilience4j` não expõe o tempo restante de forma barata; valor fixo = `BALANCE_CB_OPEN_WAIT`).

### R-13. Observabilidade (critério: production readiness)

- **Decision**: contrato em `contracts/observability.md`. Actuator com `liveness` (só `livenessState`), `readiness` (**só `readinessState`**:
  estado do próprio processo) e um grupo **separado** `dependencies` (`/actuator/health/dependencies`, com `DynamoDbHealthIndicator`: probe
  `DescribeTable` com cache de 5 s e timeout curto; `show-details=never`) mais o gauge `balance.dependency.up{dependency="dynamodb"}` (1/0) para
  alerta; tudo em **porta separada** (`MANAGEMENT_SERVER_PORT=8082`). O estado do DynamoDB é observável mas **não retira a instância de rotação**
  (decisão do usuário na revisão do plano); Micrometer + Prometheus; `balance.events{outcome,reason}` (exatamente um desfecho por mensagem),
  timers de escrita/leitura/ingestão com histograma (p50/p99 via `histogram_quantile`), `http.server.requests` com histograma/SLO;
  logs JSON nativos (`logging.structured.format.console=logstash`) com MDC `correlationId`, `accountId`, `transactionId`; sem saldo,
  titular, payload nem mensagens de exceção de parsers.
- **Rationale**: *[Spike]* Boot 4.1.0 emite JSON estruturado nativo com MDC como chaves de topo, `/actuator/health/{liveness,readiness}`
  respondem `UP` e o Prometheus exporta métricas JVM + resilience4j. Porta separada evita expor actuator na porta pública sem
  autenticação (a segurança do endpoint é do gateway, spec Assumptions). Contar `rejected` só após o DLT confirmar evita dupla contagem
  em tentativas de publicação. **Readiness sem dependência compartilhada**: com o DynamoDB na readiness, todas as instâncias saem do
  balanceador ao mesmo tempo e o cliente perde o 503 rápido com `Retry-After`/Problem Details (anula o circuit breaker e o SC-008); a
  documentação do Spring Boot desaconselha dependências externas compartilhadas na readiness. A API já responde a indisponibilidade de
  forma explícita, e o consumer faz backpressure; a saúde da dependência vai ao grupo `dependencies` + métrica.
- **Alternatives**: DynamoDB na readiness (proposta original; descartada pelo usuário, ver acima); percentis client-side (`percentiles`) — não
  agregáveis entre instâncias; probe via `GetItem` (consome RCU por probe); actuator na porta 8080 (superfície pública).

### R-14. Estratégia de testes (critério: testes)

- **Decision**:
  1. **TDD** (Constitution VI): teste vermelho antes do código, em commits pequenos.
  2. **Unit (`test`, sem infra)**: domínio (value objects, precedência), parser (uma linha de tabela por defeito da seção 3 de
     `kafka-events.md`), application (fakes), adapters (SDK/Kafka mockados), controller com `@WebMvcTest` + `@MockitoBean`, Konsist.
     **Correção do contexto de teste**: `ApplicationTests` usa `@ActiveProfiles("test")` com `application-test.yaml`
     (`spring.kafka.listener.auto-startup=false`) — *[Spike]* o contexto completo (listener, `KafkaTemplate`, error handler) sobe
     com o broker **desligado**; `GreetingControllerTest` desaparece com o `hello`.
  3. **Propriedade** (Constitution VI): **kotest-property 6.2.5** — para qualquer conjunto de eventos da mesma conta, qualquer permutação e
     qualquer duplicação/intercalação resulta no mesmo snapshot final = evento de maior `(ts, txId)`; inclui empates e DECLINED/DISABLED.
     `checkAll` com `seed` fixa (reprodutível). Roda sobre o `InMemoryBalanceStore`; uma versão reduzida roda no DynamoDB Local.
  4. **Contrato fake x real**: `BalanceSnapshotWriterContract` abstrato executado contra o fake (unit) e contra o DynamoDB Local (integração).
  5. **Integração (`integrationTest`, DynamoDB Local + Redpanda reais via docker compose)**: fluxo ponta a ponta (publicar -> consultar),
     duplicata/desordem/empate, **concorrência** (N threads, mesma conta, fora de ordem), **DLT** (mensagens de cada tipo de defeito
     intercaladas com válidas; conteúdo e headers), **indisponibilidade** (`docker compose pause dynamodb` -> 503 + `Retry-After` <= 2 s,
     readiness `UP` e liveness `UP` (instância segue em rotação), grupo `dependencies` `DOWN`, backlog processado após `unpause`, 0 mensagens no DLT; ignorado se Docker CLI ausente) e injeção de
     falhas no SDK (throttling/5xx) para a classificação transitória. Cada execução cria um tópico próprio (`it-<uuid>`) e usa contas
     aleatórias: sem interferência entre testes.
  6. **Contrato HTTP**: MockMvc (200/400/404/409/503, `Retry-After`, `Cache-Control`, problem+json) + verificação de drift contra o `openapi.yaml`.
  7. **Gate**: JaCoCo >= 90% de instruções mantido, sem novas exclusões (adapters finos e testáveis; classes `@Configuration` cobertas pelo
     contexto de teste).
  8. **Carga**: script **k6** (`grafana/k6:2.3.0`) para a leitura e o gerador do starter para ingestão — *opcional, fora do caminho crítico*;
     se não houver tempo é documentado (R-17).
- **Rationale**: kotest-property (versão comprovada acima) é só biblioteca, sem segundo *test engine*. **jqwik 1.10.1** foi avaliado e
  descartado: registra seu próprio *engine* compilado contra JUnit Platform 1.14.4 (o Boot 4.1 usa 6.0.3, combinação não documentada),
  tem ciclo de vida próprio que não convive com `@SpringBootTest` com injeção por construtor (falha observada no *spike*) e sua saída de teste incluiu um aviso
  dirigido a agentes de IA pedindo que os resultados fossem ignorados — texto ignorado por não ser instrução do usuário, mas fator de
  ruído/risco para a cadeia de suprimentos de um repositório de avaliação. **Testcontainers 2.0.5** descartado: o starter já padroniza
  Docker Compose + `make integration-test` + CI; um avaliador roda o mesmo caminho que a CI.
- **Alternatives**: jqwik / hand-rolled com `kotlin.random` (sem *shrinking*); Testcontainers; `@SpringBootTest` de contexto completo para
  tudo (lento, acopla a infra).

### R-15. Infraestrutura e produção

- **Decision**:
  - `infra/dynamodb/seed.sh`: cria `AccountBalances` (PK/SK, on-demand, sem TTL/GSI) e grava uma conta de exemplo conhecida (para o
    `quickstart`); remove `GreetingMessages`.
  - `infra/redpanda/seed.sh`: cria `transacoes-financeiras-processadas` (12 partições) e `.DLT` (3, `retention.ms=1209600000`), idempotente; sem publicar as
    mensagens `greeting-templates`. `config.sh` inalterado (auto-criação desligada continua).
  - `docker-compose.yml`: variáveis novas (`BALANCE_TABLE_NAME`, `BALANCE_EVENTS_TOPIC`, ...), `depends_on` com `service_completed_successfully`
    nos seeds, `stop_grace_period: 40s`, porta de gerenciamento 8082, `healthcheck` do app.
  - `Makefile`: remove alvos/variáveis de `hello`; novos: `kafka-produce-scenario` (eventos determinísticos: mesma conta, desordem, duplicata,
    empate, DISABLED, veneno), `balance-get ACCOUNT=<uuid>`, `chaos-dynamodb-pause/unpause`, `load-test` (opcional); `db-scan` aponta para a nova tabela.
  - `Dockerfile`: usuário **não-root** (uid 10001), flags da JVM `-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError` **no `ENTRYPOINT`** (a versão inicial usava `JAVA_TOOL_OPTIONS`; a linha
    `Picked up JAVA_TOOL_OPTIONS` da JVM não é JSON e quebraria o log estruturado; corrigido na Phase 11 do `tasks.md`),
    `HEALTHCHECK` (curl está na imagem `eclipse-temurin:21-jre`, verificado) em `/actuator/health/liveness`, `EXPOSE 8080 8082`, ENTRYPOINT em
    *exec form* (JVM recebe SIGTERM e o graceful shutdown funciona). **Tags fixas** `eclipse-temurin:21.0.12_8-jdk-noble`/`-jre-noble`
    (a atual `21-jre` flutua; tag fixa exige rotina de atualização) — *decisão aprovada pelo autor na revisão do plano*.
  - `DynamoDbConfig`: `endpointOverride` e credenciais estáticas somente se `DYNAMODB_ENDPOINT` estiver definido; senão `DefaultCredentialsProvider` (Constitution: sem segredos; chain fora do local).
  - CI (`build`, `test`, `docker`, `codeql`): sem mudança de estrutura. O job de integração continua `docker compose up dynamodb dynamodb-seed redpanda redpanda-seed`;
    imagem `itau-hello-world` renomeada para `consulta-saldo` em Makefile e `docker.yml` (opcional).
- **Alternatives**: manter tags flutuantes (patches automáticos, porém não reprodutível); actuator na 8080; Prometheus+Grafana no compose (não
  é requisito; documentado como evolução).

### R-16. Tracing distribuído

- **Decision**: **não implementar OpenTelemetry na v1; documentar como evolução.** Correlação por MDC (`correlationId`) nos logs e no
  cabeçalho `X-Correlation-Id` (API) e `topic-partition@offset` (consumer).
- **Rationale**: Constitution VII pede "trace id em contexto" nos logs: o `correlationId` cumpre. `spring-boot-starter-opentelemetry` 4.1.0
  existe, mas exige coletor OTLP no compose, amostragem e CPU; o autorizador externo não propaga `traceparent` (mensagens sem cabeçalhos),
  então cada mensagem iniciaria um trace isolado — pouco valor incremental. Desenho de evolução: habilitar o starter, `management.tracing.sampling.probability`,
  `spring.kafka.listener.observation-enabled=true`, exportador OTLP; o MDC passa a receber `traceId`/`spanId` sem mudar o código.
- **Alternatives**: implementar agora (custo e superfície sem requisito), Zipkin/Brave (menos alinhado ao Boot 4).

### R-17. O que não será implementado (documentado com motivação; Constitution VIII e enunciado)

| Item | Motivação | Desenho proposto |
|------|-----------|------------------|
| Ledger `TX#` com TTL | Dobra WCU e concentra escrita na partição da conta; spec não expõe histórico; corretude independe | Duas escritas independentes e idempotentes (R-03) |
| GSI por titular | Sem padrão de acesso; +WCU por escrita | `gsi1pk=OWNER#id`, `gsi1sk=ACCOUNT#id`, esparso, projeção parcial |
| Write sharding / coalescência | Sem contas quentes na carga de referência | R-09 e `data-model.md` 4.5; gatilho por throttling/`obsolete` |
| Reprocessamento automático do DLT | Spec: manual nesta versão | Roteiro manual em `kafka-events.md` seção 7; futuro `retry` controlado |
| Tracing OTel | R-16 | Starter + OTLP |
| Prometheus/Grafana/alertas no compose | Não requisito; métricas expostas | Regras de alerta sugeridas: `balance.dlt.publish.failures>0`, `rejected` rate, lag, CB aberto |
| IaC (Terraform/CDK), PITR, deletion protection | Fora do escopo do repositório de avaliação | Documentado em `data-model.md` 4.1 |
| Autenticação/autorização, rate limiting da API | Spec: tratados no gateway/rede | — |
| Schema Registry/Avro | Contrato JSON imposto pelo cliente | — |
| Teste de mutação (PIT), carga sustentada multi-instância | Tempo; sem gate no starter | k6 opcional (R-14) |
| Multi-região / *global tables* | Fora da carga de referência | Chave de precedência já é determinística (converge sem coordenação) |

---

## 3. ADRs a escrever na implementação (`docs/adr/`)

Cada ADR: contexto, decisão, alternativas, consequências (Constitution VIII). Numeração sugerida:

| # | Título | Decisão em 1 linha |
|---|--------|--------------------|
| 0001 | Arquitetura hexagonal por bounded context e verificação Konsist ampliada | Contexto `balance` com domain/port/application/adapter/config; `hello` removido; Konsist cobre todos os contextos e imports proibidos |
| 0002 | Modelagem DynamoDB de snapshot por conta | Tabela única `AccountBalances`, `pk=ACCOUNT#id`/`sk=BALANCE`, on-demand, sem GSI nem ledger na v1 |
| 0003 | Precedência determinística e escrita condicional atômica | `(timestamp µs, txId)` com `UpdateItem`+`ConditionExpression`; sem RMW, sem lock local |
| 0004 | Classificação de desfechos duplicado x obsoleto | `ALL_OLD` no `ConditionalCheckFailed`; duplicado = igual ao vigente; anomalia se conteúdo divergir |
| 0005 | Representação de dinheiro e tempo | `BigDecimal` ponta a ponta, `N` no DynamoDB (`toPlainString`/`BigDecimal(String)`), escala completada às casas da moeda só na resposta (sem arredondar); timestamps µs `Long`; `updated_at` ISO com offset `America/Sao_Paulo` |
| 0006 | Validação estrita de eventos e catálogo de motivos | Parser de árvore no adapter; 7 motivos + `unprocessable_event`; tolerância de futuro 5 min; mínimo 2000-01-01 para `transaction.timestamp` e 1900-01-01 para `account.created_at` |
| 0007 | Consumer Kafka at-least-once e particionamento | Listener por registro, bytes verbatim, commit em lote após persistir, 12/3 partições, sem suposição de ordem |
| 0008 | Erros transitórios x permanentes, backpressure e DLT | Não-retentável -> DLT com headers; transitório -> backoff exponencial+jitter com pausa do container; DLT fora = não confirma |
| 0009 | Uma camada de retry por chamada e clientes DynamoDB separados | Escrita: retry só no consumer; leitura: SDK `standard` 2 tentativas; timeouts e pools isolados |
| 0010 | Circuit breaker na leitura com Resilience4j programático | Core 2.4.0 em decorator do port; 503 + `Retry-After`; nativo do Spring 7 não tem CB |
| 0011 | Consistência de leitura forte | `ConsistentRead=true` (flag), custo 2x aceito para FR-027 |
| 0012 | API, Problem Details e OpenAPI contract-first | `type` URN estável; 400/404/409/503/500; `openapi.yaml` estático com teste anti-drift; sem springdoc |
| 0013 | Observabilidade | Actuator em porta separada, readiness = só estado do app, dependências em grupo `/health/dependencies` + gauge (sem tirar a instância de rotação), Micrometer/Prometheus, desfecho único por mensagem, logs JSON com MDC; tracing como evolução |
| 0014 | Estratégia de testes e evidência de corretude | TDD; kotest-property; contrato fake x DynamoDB Local; integração via compose; chaos por `docker compose pause`; gate 90% |
| 0015 | Empacotamento e operação | Dockerfile não-root, `MaxRAMPercentage`, healthcheck, graceful shutdown, tags fixas |

## 4. Resolução das pendências da spec

| Pendência (Assumptions/Edge cases) | Resolução |
|------------------------------------|-----------|
| Duplicado x obsoleto | R-04 |
| Chave de precedência e ordem canônica | `(timestamp µs, txId lexicográfico minúsculo)` — R-04, `data-model.md` 2.1 |
| Intervalo plausível de timestamps | `transaction.timestamp` >= 2000-01-01; `account.created_at` >= 1900-01-01; futuro por tolerância em ambos — R-06 |
| Valor padrão da tolerância de futuro | `PT5M` configurável — R-06 |
| Fuso de `updated_at` | `America/Sao_Paulo` configurável — R-06 |
| Mesmo `transaction.id` + timestamp com conteúdo divergente | `duplicate` + anomalia — R-04 |
| Partições, retries, circuit breaker, modelagem | R-03, R-07..R-11 |
| Retenção de mensagens rejeitadas | DLT 14 dias — `kafka-events.md` |
| Carga de referência | Premissa do autor (o enunciado não fixa volume): 1.000 ev/s e 500 consultas/s usados no dimensionamento; ver seção 5 |

## 5. Decisões que dependiam do usuário e o que foi decidido

Todas as decisões abaixo foram propostas nesta pesquisa e **aprovadas pelo autor na revisão do plano** (`docs/metodologia-ia.md`, "Decisões tomadas pelo autor humano"); os itens 2 e 3 têm decisão explícita do autor, registrada no próprio item. O item 11 não foi validado com o cliente: é premissa do autor.

1. **Remover o exemplo `hello`** (código, testes, seeds, `http/hello.http`) e generalizar o teste Konsist (R-01).
2. ~~`balanceAmount` como `S`~~ **Decidido pelo usuário: `N` + `BigDecimal`**, escala completada às casas da moeda na resposta (R-06).
3. **Tolerância de futuro `PT5M`**, **mínimo 2000-01-01** para `transaction.timestamp` e **mínimo 1900-01-01** para `account.created_at` (R-06; aprovado pelo usuário, com a correção da revisão).
4. **Fuso `America/Sao_Paulo`** e formato `ISO_OFFSET_DATE_TIME` (fração sem zeros à direita e ausente quando zero; ver ADR-0005) (R-06).
5. **12 partições** (principal) e **3** (DLT); **retenção do DLT 14 dias**; RF 3 em produção (R-07).
6. **Sem ledger, sem GSI** na v1 (R-03) e **sem coalescência** (R-09).
7. **Tags Docker fixas** (`21.0.12_8-*-noble`) e renomear a imagem `itau-hello-world` (R-15).
8. **Porta de gerenciamento 8082** separada da API (R-13).
9. **Sem OpenTelemetry** e **sem springdoc** (R-12, R-16).
10. **k6** como teste de carga opcional (R-14).
11. Carga de referência de 1.000 ev/s e 500 req/s: **premissa do autor; o enunciado não fixa volume** (Assumptions da spec). Usada só no dimensionamento (12 partições, on-demand) e para dar sentido a SC-001/SC-002.

## 6. Riscos e itens em aberto

| # | Risco | Mitigação |
|---|-------|-----------|
| R1 | `resilience4j-micrometer` compilada contra Micrometer 1.16 e executada com 1.17 | Comprovado no spike; teste de integração de métricas do CB na implementação |
| R2 | Sem DLT, a publicação é retentada sem backoff exponencial próprio (limitada por `max.block.ms`) e o laço de reentrega ocupa a thread de consumo inteira, não só a partição | Alerta em `balance.dlt.publish.failures`; evolução: `BackOff` dedicado (ADR-0008) |
| R3 | DynamoDB Local não emula throttling/5xx/latência: a classificação transitória só se prova com injeção de falhas no SDK | Testes com cliente decorado que lança as exceções reais do SDK; outage real via `docker compose pause` |
| R4 | Nulidade JSpecify do Spring Kafka 4.1 (`RetryListener.failedDelivery(record, Exception?, int)`) surpreende em Kotlin | Anotado; usar `Exception?` (comprovado no spike) |
| R5 | Cobertura JaCoCo com classes geradas do Kotlin (`data class`, `value class`) pode derrubar o gate | Manter modelos enxutos; testar `equals/hashCode` só onde há regra; sem exclusões novas |
| R6 | Escrita com condição falsa consome WCU: tráfego muito obsoleto encarece | Métrica `obsolete`; evolução: coalescência (R-09) |
| R7 | Leitura forte indisponível durante falhas de partição do DynamoDB (vira 503) | Comportamento desejado (FR-030); flag para leitura eventual |
| R8 | O gerador do starter usa conta aleatória por evento: nunca produz desordem por conta | `make kafka-produce-scenario` (tarefa) gera casos determinísticos |
| R9 | `HealthIndicator` (grupo `dependencies`) chamando `DescribeTable` pode competir com limites de plano de controle | Cache de 5 s; alternativa `GetItem` de chave inexistente |
| R10 | Constitution proíbe co-autoria de IA em commits; o contexto de execução sugere trailer de atribuição | A Constitution prevalece (Governance); conferir antes de commitar |
| R11 | A pausa do backoff vale para todas as threads da instância (container pai), excessiva para *throttling* de uma conta quente | Correta para indisponibilidade geral; evolução: pausa por thread ou partição (ADR-0008) |
| R12 | Defeito sistêmico manda mensagens válidas ao DLT como `unprocessable_event` | Alerta em `rate(balance_events_total{reason="unprocessable_event"}[5m]) > 0` e replay manual; evolução: fusível por taxa (ADR-0008) |
| R13 | Limites de tamanho do DLT (`max.request.size`, `max.message.bytes`) e invariantes de configuração (`max.poll.records x write timeout < max.poll.interval`, `dlt != topic`) não validados na partida | Invariante coberto por teste com os valores padrão; evolução: validar na partida (README, "Riscos conhecidos") |

## 7. Cobertura dos critérios de avaliação do cliente

| Critério | Onde é respondido |
|----------|-------------------|
| Modelagem DynamoDB (PK, SK, índices) | R-03, `data-model.md` 4 |
| Concorrência (saldo reflete a transação mais recente mesmo fora de ordem) | R-04, `data-model.md` 2.1/4.4, R-14 (propriedade + concorrência real) |
| Resiliência (retries, backoff, circuit breaker) | R-08, R-10, R-11 |
| Testes (fluxos principais e corner cases) | R-14, `quickstart.md` |
| Qualidade e aderência hexagonal | R-01, R-02, plan.md (Constitution Check) |
| Cenários adversos | `kafka-events.md` (7 motivos), R-08, `quickstart.md` cenários 6-9 |
| Production readiness (logs, métricas, conteinerização) | R-13, R-15, `contracts/observability.md`, `contracts/configuration.md` |
| O que não coube: documentar com motivação | R-17 |

## 8. Apêndice: resultados dos spikes

| Spike | Resultado |
|-------|-----------|
| DynamoDB `N` x `S` | `183.10` -> `N`=`183.1` (esperado, coberto por teste); 39 dígitos em `N` -> `DynamoDbException` 400 (domínio impõe o teto de 38); `S` preservaria a escala (recusado) |
| Condição + `ALL_OLD` | `ConditionalCheckFailedException.hasItem()==true` com o item vigente, em 3.3.0 |
| Empate de timestamp | maior `txId` vence; menor perde; nova ordem `newer` aplica |
| Concorrência | 400 escritas/32 threads/uma conta: 7 aplicadas, 393 condicionais falsas, 0 outras; vencedor correto |
| Ordem de UUID | 2.000 UUIDs: 0 divergências banco x `String.compareTo`; `UUID.fromString("1-1-1-1-1")` é aceito |
| Cliente DynamoDB default | `maxAttempts=9`, base 25 ms, throttling 500 ms, cota de retry ligada |
| Falha de conexão | recusada em ~2 ms; buraco negro em ~500 ms (`connectionTimeout` 500 ms) |
| Throughput local (DynamoDB Local) | 1.330/s (1 thread), 3.739/s (4), 3.282/s (8) |
| Jackson 3 | floats -> `DecimalNode`; chaves duplicadas, UTF-8 inválido, lixo final, NaN, número > 1000 chars, profundidade > 500 -> exceções; `1E999999999` -> escala -999999999 (exige validação antes de expandir) |
| Serialização | sem `WRITE_BIGDECIMAL_AS_PLAIN`: `1E+3`; com: `1000`; `183.10` (escala 2) sai plano |
| Kafka (Redpanda) | veneno -> DLT (3 partições, destino `-1`); transitória: pausa, 0 no DLT, retomada ~1 s; DLT ausente: não confirma e drena após criação; não classificada: 3 entregas -> DLT |
| Boot/Actuator | JSON estruturado nativo, liveness/readiness `UP`, Prometheus com JVM+resilience4j, springdoc 3.1.1 gera OpenAPI 3.1.0 |
| Contexto sem broker | `spring.kafka.listener.auto-startup=false`: contexto completo sobe com o Redpanda parado |
