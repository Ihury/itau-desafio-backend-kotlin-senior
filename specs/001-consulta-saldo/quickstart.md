# Quickstart: validação ponta a ponta da Consulta de Saldo

**Feature**: `001-consulta-saldo` | **Plano**: [plan.md](./plan.md) | **Contratos**: [contracts/](./contracts/) | **Modelo**: [data-model.md](./data-model.md)

Guia de **validação e execução** (não de implementação). Cada cenário prova um requisito/critério de sucesso da spec.
Os alvos `make` marcados com **(novo)** e o serviço em si são entregues na fase de implementação (`/speckit-tasks` -> `/speckit-implement`);
este guia define o comportamento esperado que a implementação deve satisfazer.

## 0. Pré-requisitos

- Docker com Docker Compose, `make`, `curl`, JDK 21 (para `./gradlew` local; `make test` roda tudo em container).
- Portas livres: 8080 (API), 8082 (gerenciamento), 8000 (DynamoDB Local), 8001 (admin), 19092 (Redpanda), 8081 (Console).
- Referências de contrato: `contracts/openapi.yaml` (API), `contracts/kafka-events.md` (validação/DLT), `contracts/observability.md` (métricas).

## 1. Testes automatizados (sem infraestrutura) — Constitution VI

```bash
./gradlew check            # ou: make test (dentro de container)
```

Esperado: verde; suíte **sem broker nem banco** (o contexto Spring de teste sobe com `spring.kafka.listener.auto-startup=false`);
teste de arquitetura Konsist cobrindo todos os contextos; teste de propriedade de convergência; gate JaCoCo >= 90% de instruções.

## 2. Subir a stack e verificar prontidão (SC-012)

```bash
make up                                                     # app + DynamoDB Local + Redpanda + seeds
curl -s localhost:8082/actuator/health/liveness             # {"status":"UP"}
curl -s localhost:8082/actuator/health/readiness            # {"status":"UP"} (estado do próprio app)
curl -s localhost:8082/actuator/health/dependencies         # {"status":"UP"} (DynamoDB)
docker compose run --rm --entrypoint rpk redpanda-seed topic list --brokers redpanda:9092
```

Esperado no `topic list`: `transacoes-financeiras-processadas` (12 partições) e `transacoes-financeiras-processadas.DLT` (3).
A tabela `AccountBalances` existe com uma conta de exemplo (seed).

## 3. Smoke test com a conta de exemplo do enunciado (US1)

```bash
curl -i localhost:8080/balances/5b19c8b6-0cc4-4c72-a989-0c2ee15fa975
```

Esperado: `200`, `Content-Type: application/json`, `Cache-Control: no-store`, `X-Correlation-Id` e corpo idêntico ao exemplo do cliente:

```json
{"id":"5b19c8b6-0cc4-4c72-a989-0c2ee15fa975","owner":"315e3cfe-f4af-4cd2-b298-a449e614349a","balance":{"amount":183.12,"currency":"BRL"},"updated_at":"2025-07-05T18:04:13.433-03:00"}
```

## 4. Variáveis e helpers dos cenários

Dados de teste determinísticos (contas, transações e instantes fixos em µs). Cole no shell:

```bash
OWN=315e3cfe-f4af-4cd2-b298-a449e614349a
T0=1751749453433123                                    # 2025-07-05T18:04:13.433123-03:00
t() { echo $(( T0 + $1 * 1000000 )); }                  # t N = T0 + N segundos
TOPIC=transacoes-financeiras-processadas
BASE='{"transaction":{"id":"@TX@","type":"CREDIT","amount":10.00,"currency":"BRL","status":"@TXST@","timestamp":@TS@},"account":{"id":"@ACC@","owner":"'$OWN'","created_at":1634874339000000,"status":"@ST@","balance":{"amount":@BAL@,"currency":"BRL"}}}'
# ev <txId> <timestampµs> <accountId> <accountStatus> <balance> [txStatus]
ev() { echo "$BASE" | sed -e "s/@TX@/$1/" -e "s/@TS@/$2/" -e "s/@ACC@/$3/" -e "s/@ST@/$4/" -e "s/@BAL@/$5/" -e "s/@TXST@/${6:-APPROVED}/"; }
produce() { docker compose run --rm -T --entrypoint rpk redpanda-seed topic produce $TOPIC --brokers redpanda:9092 -f '%v\n'; }
metric() { curl -s localhost:8082/actuator/prometheus | grep -E "^$1" ; }
tx() { printf '00000000-0000-4000-8000-%012d' $1; }     # tx 1 .. tx N
dlt_total() { docker compose run --rm -T --entrypoint rpk redpanda-seed topic describe $TOPIC.DLT -p --brokers redpanda:9092 | awk 'NR>1{s+=$NF} END{print s+0}'; }
A=aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1; B=bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1; C=cccccccc-cccc-4ccc-8ccc-ccccccccccc1
D=dddddddd-dddd-4ddd-8ddd-ddddddddddd1
C2=cccccccc-cccc-4ccc-8ccc-ccccccccccc2; E=eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee2
```

## 5. Cenários de corretude (US2, US3) — Constitution II/III

### 5.1 Desordem + duplicidade (SC-003, SC-004) — conta `A`

```bash
{ ev $(tx 3) $(t 3) $A ENABLED 300.00
  ev $(tx 1) $(t 1) $A ENABLED 100.00      # mais antigo, chega depois
  ev $(tx 2) $(t 2) $A ENABLED 200.00      # mais antigo, chega depois
  ev $(tx 3) $(t 3) $A ENABLED 300.00      # duplicata
} | produce
sleep 3; curl -s localhost:8080/balances/$A
```

Esperado: `{"id":"<A>","owner":"<OWN>","balance":{"amount":300.00,"currency":"BRL"},"updated_at":"2025-07-05T18:04:16.433123-03:00"}`
(microssegundos preservados; saldo do evento de maior instante mesmo tendo chegado primeiro). Métricas
(`metric balance_events_total`): `processed`=1, `obsolete`=2, `duplicate`=1 para esta rodada — a soma (4) é igual às mensagens publicadas (SC-010).

### 5.2 Empate de timestamp (SC-005) — conta `C`, duas ordens de chegada

```bash
ev $(tx 11) $(t 5) $C ENABLED 20.00 | produce      # id maior (…0011)
ev $(tx 10) $(t 5) $C ENABLED 10.00 | produce      # mesmo ts, id menor -> obsoleto
sleep 2; curl -s localhost:8080/balances/$C
```

Esperado: `amount` = **20.00** (vence o maior `transactionId`). Repetindo com a ordem trocada em outra conta (`C2`), o resultado é o mesmo.

### 5.3 DECLINED atualiza o snapshot (FR-010) e precisão (FR-018/019, SC-009) — conta `D`

```bash
ev $(tx 20) $(t 1) $D ENABLED 12345678901234567890.123456789012345678 | produce            # 38 dígitos, escala 18
sleep 2; curl -s localhost:8080/balances/$D                                                  # passo 1
ev $(tx 21) $(t 2) $D ENABLED 0.10 DECLINED | produce                                       # rejeitada: saldo inalterado no autorizador
sleep 2; curl -s localhost:8080/balances/$D                                                  # passo 2
ev $(tx 22) $(t 3) $D ENABLED 100 | produce; sleep 2; curl -s localhost:8080/balances/$D      # passo 3
ev $(tx 23) $(t 4) $D ENABLED 10.123 | produce; sleep 2; curl -s localhost:8080/balances/$D   # passo 4
```

Esperado: passo 1 -> `amount` = `12345678901234567890.123456789012345678` (idêntico, sem notação científica nem arredondamento) e
`updated_at` `...:14.433123-03:00`; passo 2 -> o evento DECLINED, por ter precedência maior, atualiza o snapshot: `amount` = `0.10`
(o banco guarda `0.1`; a resposta completa a escala às 2 casas do BRL) e `updated_at` `...:15.433123-03:00`; passo 3 -> `100.00` (entrada `100`);
passo 4 -> `10.123` (mais casas que a moeda: **nunca arredonda**). Valores numericamente idênticos aos informados (FR-018).

### 5.4 Conta DISABLED (FR-011, SC-013) — conta `B`

```bash
ev $(tx 30) $(t 1) $B ENABLED 50.00  | produce; sleep 2; curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/balances/$B   # 200
ev $(tx 31) $(t 2) $B DISABLED 50.00 | produce; sleep 2; curl -si localhost:8080/balances/$B | sed -n '1p;/^{/p'            # 409
ev $(tx 29) $(t 0) $B ENABLED 40.00  | produce; sleep 2; curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/balances/$B # ainda 409 (obsoleto)
ev $(tx 32) $(t 3) $B ENABLED 70.00  | produce; sleep 2; curl -s localhost:8080/balances/$B                                  # 200, amount 70.00
```

Esperado: 200 -> **409** com `application/problem+json` e `type` `urn:problem-type:consulta-saldo:conta-desabilitada` **sem saldo nem titular** -> 409 (evento antigo não muda) -> 200 com 70.00.

### 5.5 Moeda do saldo diferente da moeda da transação (FR-020) — conta `E`

```bash
ev $(tx 70) $(t 1) $E ENABLED 25.00 | sed 's/"balance":{"amount":25.00,"currency":"BRL"}/"balance":{"amount":25.00,"currency":"USD"}/' | produce
sleep 2; curl -s localhost:8080/balances/$E
```

Esperado: `200` com `"balance":{"amount":25.00,"currency":"USD"}` (a transação é BRL; a moeda exposta é sempre a do saldo, sem conversão nem rejeição) e nenhuma mensagem nova no DLT.

## 6. Mensagens inválidas e isolamento (US4, SC-006)

```bash
V=$(ev $(tx 40) $(t 1) eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee1 ENABLED 5.00)      # válida, no meio dos defeitos
NOW=$(date +%s)
{ echo '{not json'                                                               # malformed_payload
  echo "$V" | sed 's/"owner":"[^"]*",//'                                         # missing_field
  echo "$V" | sed 's/eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee1/1-1-1-1-1/'           # invalid_identifier
  echo "$V" | sed 's/"currency":"BRL"/"currency":"brl"/'                         # invalid_currency
  echo "$V" | sed 's/"amount":10.00/"amount":"10.00"/'                           # invalid_value (string)
  echo "$V" | sed "s/\"timestamp\":$(t 1)/\"timestamp\":1751749453433/"          # invalid_timestamp (milissegundos)
  echo "$V" | sed "s/\"timestamp\":$(t 1)/\"timestamp\":$(( (NOW + 3600) * 1000000 ))/"  # invalid_timestamp (futuro > 5 min)
  echo "$V" | sed 's/"type":"CREDIT"/"type":"TRANSFER"/'                         # unknown_domain_value
  echo "$V" | sed 's/"status":"ENABLED"/"status":"SUSPENDED"/'                   # unknown_domain_value
  echo "$V"                                                                      # válida: deve ser processada
  ev $(tx 41) $(( (NOW + 60) * 1000000 )) ffffffff-ffff-4fff-8fff-fffffffffff1 ENABLED 9.00   # futuro dentro da tolerância: processada
} | produce
sleep 5
dlt_total                                                                        # 9 (se o DLT estava vazio)
# -n limita a leitura ao total do DLT (sem -n o consume nunca termina); -a porque a saida tem bytes binarios
docker compose run --rm -T --entrypoint rpk redpanda-seed topic consume $TOPIC.DLT --brokers redpanda:9092 -o start -n "$(dlt_total)" -f '%h{%k=%v;} | %v\n' \
  | grep -ao 'x-rejection-reason=[a-z_]*' | sort | uniq -c
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/balances/eeeeeeee-eeee-4eee-8eee-eeeeeeeeeee1    # 200 (a válida)
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/balances/ffffffff-ffff-4fff-8fff-fffffffffff1    # 200 (dentro da tolerância)
```

Esperado: 9 mensagens no DLT (`malformed_payload`=1, `missing_field`=1, `invalid_identifier`=1, `invalid_currency`=1, `invalid_value`=1,
`invalid_timestamp`=2, `unknown_domain_value`=2), **valor original preservado** (`-f '%v'`), nenhum saldo alterado por elas, as duas válidas
processadas e `balance_events_total{outcome="rejected",reason=...}` batendo com as contagens.

Notas sobre a leitura do DLT: (a) o `rpk topic consume` acompanha o topico e nao termina sozinho, por isso o `-n` (aqui o total do DLT, via `dlt_total`);
sem ele o comando fica preso, e `make kafka-consume TOPIC=$TOPIC.DLT` e a alternativa com timeout de 5 s (mas imprime so o valor, sem cabecalhos);
(b) a saida contem bytes binarios (cabecalhos `kafka_dlt-original-partition/offset/timestamp` em big-endian e mensagens-veneno com bytes invalidos em UTF-8,
que tambem vao ao DLT como `malformed_payload`), e o `grep` trata esse fluxo como binario e nao imprime as linhas: use `grep -a` (ou `grep -ao`).
Se o DLT tiver mensagens de execucoes anteriores, as contagens por motivo refletem tudo o que ele acumulou.

### 6.1 Conta criada antes de 2000 (`account.created_at` legítimo) — conta `G`

```bash
G=99999999-9999-4999-8999-999999999991
ev $(tx 60) $(t 1) $G ENABLED 15.00 | sed 's/"created_at":1634874339000000/"created_at":899251200000000/' | produce   # conta criada em 1998-07-01
sleep 2; curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/balances/$G                                              # 200
```

Esperado: **200** com o saldo 15.00 e nenhuma mensagem nova no DLT (`dlt_total` inalterado): `account.created_at` só exige mínimo
1900-01-01 (o mínimo de 2000 vale apenas para `transaction.timestamp`). Um `created_at` de 1850 iria ao DLT com `invalid_timestamp`.

## 7. Cenários adversos da API (US1) — Constitution IV, V

```bash
curl -si localhost:8080/balances/abc                                        # 400 requisicao-invalida (armazenamento não consultado)
curl -si localhost:8080/balances/1-1-1-1-1                                  # 400 (UUID "lenient" do JDK NÃO é aceito)
curl -si localhost:8080/balances/99999999-9999-4999-8999-999999999999       # 404 conta-nao-encontrada (nunca saldo zerado)
curl -si -H 'X-Correlation-Id: teste-123' localhost:8080/balances/$A | grep -i correlation   # eco do cabeçalho
```

Esperado: todos os erros em `application/problem+json` com `type` estável e distinguível (`contracts/openapi.yaml`); sem pilha nem nomes de infraestrutura.

## 8. Indisponibilidade do armazenamento (US5, SC-007, SC-008)

```bash
docker compose pause dynamodb                                                # armazenamento "fora do ar" (chaos)
DLT_ANTES=$(dlt_total)
ev $(tx 50) $(t 9) $A ENABLED 999.00 | produce
time curl -si localhost:8080/balances/$A | sed -n '1p;/^Retry-After/Ip'      # 503 + Retry-After em <= 2 s (~1,3 s, timeout)
curl -s -o /dev/null -w '%{http_code}\n' localhost:8082/actuator/health/dependencies # 503 (após <= 5 s de cache do probe)
curl -s -o /dev/null -w '%{http_code}\n' localhost:8082/actuator/health/readiness   # 200 (a instância continua em rotação)
curl -s -o /dev/null -w '%{http_code}\n' localhost:8082/actuator/health/liveness    # 200 (liveness independe do banco)
metric balance_consumer_backpressure_total ; metric 'resilience4j_circuitbreaker_state.*state="open"'   # ainda 0.0: poucas chamadas
# rajada concorrente (>= 20 chamadas na janela de 10 s) para abrir o circuit breaker
seq 40 | xargs -P 40 -I{} curl -s -o /dev/null -w '%{http_code} %{time_total}s\n' localhost:8080/balances/$A | sort | uniq -c   # 40 x 503, cada um ~1,3 s
metric 'resilience4j_circuitbreaker_state.*state="open"'                       # 1.0: circuito aberto
time curl -si localhost:8080/balances/$A | sed -n '1p;/^Retry-After/Ip'      # 503 imediato (fail-fast, ~ms)
echo "DLT antes=$DLT_ANTES depois=$(dlt_total)"                                     # iguais: nada foi isolado
docker compose run --rm --entrypoint rpk redpanda-seed group describe consulta-saldo --brokers redpanda:9092 | grep -E 'TOTAL-LAG'   # lag > 0: evento segue no broker
docker compose unpause dynamodb
sleep 45; curl -s localhost:8080/balances/$A                                 # 200 com 999.00 (evento consumido após a recuperação)
for i in 1 2 3 4 5 6; do curl -s -o /dev/null -w '%{http_code} ' localhost:8080/balances/$A; sleep 0.5; done; echo   # consultas fecham o circuito
metric 'resilience4j_circuitbreaker_state.*state="closed"'                     # 1.0
```

O circuit breaker so abre com **pelo menos 20 chamadas** na janela de 10 s (minimo configurado). Com poucas consultas sequenciais, cada uma recebe 503 por timeout
(~1,3 s, dentro do SC-008) e o circuito continua `closed`; por isso a rajada concorrente acima (30 a 40 consultas em paralelo) e o que o abre, e so entao a consulta
seguinte falha em ~ms (`resilience4j_circuitbreaker_not_permitted_calls_total` > 0, o fail-fast). Com o circuito aberto, ele passa a `half_open` cerca de 10 s
apos o `unpause` e volta a `closed` com as primeiras consultas bem-sucedidas.

Esperado durante a falha: 503 com `Retry-After: 10` e corpo `servico-indisponivel` (**nunca** saldo antigo nem 404), grupo `dependencies` 503 (e `balance_dependency_up{dependency="dynamodb"}` = 0), readiness 200, liveness 200, o evento
**permanece no broker** (`TOTAL-LAG` > 0), `balance_consumer_backpressure_total` cresce, **0 mensagens novas no DLT** (`DLT antes` = `depois`). Após `unpause`: o consumer retoma sozinho
(<= ~30 s de backoff máximo), o saldo passa a 999.00, `dependencies` volta a 200 (readiness nunca deixou de ser 200) e o circuit breaker fecha (HALF_OPEN -> CLOSED, se a rajada o tiver aberto).

## 9. Reinício/encerramento gracioso (SC/US6.4, at-least-once)

```bash
make kafka-produce-transactions-events TOPIC=$TOPIC COUNT=2000 &      # contas aleatórias
sleep 5; docker compose restart app                                   # SIGTERM -> graceful shutdown; reentrega pós-restart
wait; sleep 30
docker compose run --rm --entrypoint aws dynamodb-seed dynamodb scan --table-name AccountBalances --select COUNT \
  --endpoint-url http://dynamodb:8000 --region us-east-1
```

Esperado: a contagem de itens cresce **exatamente** em 2000 (nenhum evento perdido) e o lag do grupo volta a 0 (`rpk group describe consulta-saldo`). No reinicio
gracioso o offset e confirmado **por registro**, entao normalmente **nao** ha reentrega e **nao** aparece `duplicate`; alem disso os contadores de
`balance_events_total` sao do processo e **zeram no restart** (so refletem o que a nova instancia processou). Nada e confirmado sem persistir.

Opcional, para ver `duplicate` de fato: pare o app, rebobine o grupo de consumo e suba o app de novo; tudo o que ja estava persistido e reentregue e contado como `duplicate`
(a contagem de itens nao muda).

```bash
docker compose stop app
docker compose run --rm -T --entrypoint rpk redpanda-seed group seek consulta-saldo --to start --topics $TOPIC --brokers redpanda:9092
docker compose start app; sleep 30; metric 'balance_events_total.*duplicate'                       # > 0
```

## 10. Concorrência real e propriedade contra infraestrutura real

```bash
make integration-test          # sobe DynamoDB Local + Redpanda, roda ./gradlew integrationTest
```

Esperado: verdes — ingestão ponta a ponta; duplicata/desordem/empate; **N threads na mesma conta fora de ordem** convergem para o evento de maior
`(ts, txId)`; DLT com um caso por motivo; outage por `docker compose pause`; contrato do writer (fake x DynamoDB Local); métricas do circuit breaker.

## 11. Carga (opcional — R-14)

```bash
make load-test                 # (novo) k6 (grafana/k6:2.3.0) contra GET /balances/{accountId}
```

Esperado (referência, a confirmar com o cliente): 500 req/s com p99 <= 300 ms e p50 <= 50 ms; ingestão ~1.000 ev/s com o gerador do starter
(`COUNT` alto). Se não houver tempo, o item consta como não implementado em `research.md` R-17.

## 12. Matriz de rastreabilidade

| Cenário | Requisitos / critérios |
|---------|------------------------|
| 3 | FR-021/022, US1.1, SC-009 |
| 5.1 | FR-005/006/007/008, SC-003/004/010, US3.1/3.2/3.4 |
| 5.2 | FR-004, SC-005, US3.3 |
| 5.3 | FR-010/018/019, SC-009, US2.5 |
| 5.4 | FR-011/013, SC-013, US1.6-1.8 |
| 6.1 | FR-014 (intervalo plausível por campo), US4.2 |
| 6 | FR-012/014/015/016/017, SC-006, US4 |
| 7 | FR-023/024/026, US1.2/1.3 |
| 8 | FR-025/028/029/030/033, SC-007/008, US5, US6.2 |
| 9 | FR-009/034, US6.4, US3.6 |
| 10 | Constitution VI (concorrência real, propriedade, integração) |
| 11 | SC-001/002 |
| 5.1 e 9 | FR-001, FR-002, FR-003 |
| 5.5 | FR-020 |
| 5.1 e 6 | FR-031 (métricas por desfecho) |
| Testes de privacidade de log (tasks T144/T147) | FR-032 |
| Regra Konsist de catch (tasks T145) | FR-035 |
| 5.4 e 8 | FR-027 (leitura sempre íntegra) |
| 2 | SC-011 (saúde e métricas em < 1 min), SC-012 (subir e consultar em <= 10 min) |
