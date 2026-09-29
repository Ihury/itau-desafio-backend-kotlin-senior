# Contrato Kafka: tópicos, validação e Dead Letter Topic

Interface de mensageria da feature `001-consulta-saldo`. O formato do evento de entrada é **imposto pelo cliente**
(`transaction-event.schema.json`); o restante (partições, DLT, motivos) é decisão deste plano
(ver `research.md`, R-07 a R-10).

## 1. Tópicos

| Tópico | Papel | Partições | Réplicas | Retenção | Observações |
|--------|-------|-----------|----------|----------|-------------|
| `transacoes-financeiras-processadas` | Entrada (produzido pelo autorizador, fora do escopo) | 12 (decisão proposta) | 1 local / 3 em produção | padrão do broker (7 d) | Mensagens **sem chave**; nenhuma ordenação por conta é assumida. |
| `transacoes-financeiras-processadas.DLT` | Isolamento de mensagens inválidas | 3 (decisão proposta) | 1 local / 3 em produção | 14 dias (`retention.ms=1209600000`) | Sem reprocessamento automático; reprocessamento manual (seção 6). |

- Auto-criação de tópicos está **desligada** no Redpanda do starter-kit: o seed (`infra/redpanda/seed.sh`)
  cria ambos de forma idempotente, com as partições acima.
- O DLT tem menos partições que o tópico principal: o recoverer publica com **partição não definida** (`-1`),
  deixando o particionador escolher (validado no spike; o default `mesma partição` falharia com 4 -> 3 partições).
- Aumentar partições do tópico principal é seguro: a corretude não depende de ordenação/chave (Constitution II).

## 2. Consumo

| Item | Valor |
|------|-------|
| Grupo | `consulta-saldo` (`KAFKA_CONSUMER_GROUP_ID`) |
| Deserializadores | Chave e valor `ByteArrayDeserializer` (bytes verbatim; nunca lança) |
| Auto-commit | `enable.auto.commit=false`; `AckMode=BATCH` (commit após o retorno do listener de todos os registros do poll) |
| Assignor | `CooperativeStickyAssignor` |
| `max.poll.records` / `max.poll.interval.ms` | 100 / 300000 (pior caso: 100 x `apiCallTimeout` 2 s = 200 s < 300 s) |
| Listener | registro a registro (`ConsumerRecord<ByteArray?, ByteArray?>`), `concurrency` configurável (padrão 4) |
| Semântica | at-least-once; efeito idempotente garantido pela escrita condicional no banco |

## 3. Validação (adapter `input/kafka`, parser estrito sobre árvore JSON)

Passos executados nesta ordem; a **primeira** falha determina o motivo (um único motivo por mensagem):

1. **Payload**: nulo/vazio, > 64 KiB, UTF-8 inválido, JSON malformado, chaves duplicadas, tokens após o documento,
   profundidade > 500, número com > 1000 caracteres, raiz que não é objeto, ou `transaction`/`account`/`account.balance`
   presentes com tipo diferente de objeto -> `malformed_payload`.
2. **Presença**: qualquer campo obrigatório ausente ou `null` (lista completa no schema) -> `missing_field`.
3. **Valores** (ordem fixa): `transaction.id`, `transaction.type`, `transaction.amount`, `transaction.currency`,
   `transaction.status`, `transaction.timestamp` (mín. 2000-01-01), `account.id`, `account.owner`, `account.created_at`
   (mín. 1900-01-01), `account.status`, `account.balance.amount`, `account.balance.currency`.
4. **Runtime (camada `application`, relógio injetado)**: `transaction.timestamp` e `account.created_at` além de
   `agora + tolerância` -> `invalid_timestamp`.

Campos adicionais desconhecidos são **ignorados**.

## 4. Catálogo de motivos (conjunto enumerado e estável — FR-016)

| Código (`x-rejection-reason` e tag `reason`) | Rótulo da spec | Quando |
|---|---|---|
| `malformed_payload` | formato inválido | Passo 1 |
| `missing_field` | campo obrigatório ausente | Passo 2 |
| `invalid_identifier` | identificador inválido | `transaction.id`/`account.id`/`account.owner` que não sejam string UUID canônica 8-4-4-4-12 (hex, aceita maiúsculas; normaliza para minúsculas). Parse **estrito** por regex: `UUID.fromString("1-1-1-1-1")` do JDK aceitaria formatos não canônicos. |
| `invalid_value` | valor inválido | `amount` (transação ou saldo) que não seja número JSON (string, booleano, objeto), `transaction.amount` negativo, precisão > 38 dígitos significativos (limite do `N` do DynamoDB) ou escala positiva > 38; escala negativa (`1E+3`) só é expandida se `precisão - escala <= 38` (`1E999999999` é rejeitado sem expandir). Zero e saldo negativo são válidos. |
| `invalid_currency` | moeda inválida | Não é string de 3 letras maiúsculas conhecida por `java.util.Currency` |
| `invalid_timestamp` | timestamp inválido | Não é inteiro JSON (`1751641364589998.0` e `1.75E15` são rejeitados), fora de `long`, `transaction.timestamp` `< 2000-01-01T00:00:00Z` (µs) ou `account.created_at` `< 1900-01-01T00:00:00Z` (conta de 1998 é **válida**), ou (runtime) `> agora + tolerância` |
| `unknown_domain_value` | valor de domínio desconhecido | `transaction.type` ∉ {`CREDIT`,`DEBIT`}; `transaction.status` ∉ {`APPROVED`,`DECLINED`}; `account.status` ∉ {`ENABLED`,`DISABLED`} (comparação exata, case-sensitive) |
| `unprocessable_event` | (interno) | Exceção **não classificada** durante o processamento de mensagem válida (defeito nosso ou rejeição de validação do armazenamento): 3 entregas (2 retentativas de 100 ms) e então DLT, para nunca bloquear a partição indefinidamente por um defeito determinístico |

## 5. Contrato da mensagem no DLT

- **Valor e chave**: bytes originais, sem modificação (inclusive binários/UTF-8 inválido).
- **Headers**:

| Header | Conteúdo |
|--------|----------|
| `x-rejection-reason` | Código da seção 4 |
| `x-rejection-detail` | Caminho do campo ou texto estático curto (ex.: `transaction.currency`); **nunca** valores do payload |
| `x-rejected-at` | Instante (ISO 8601 UTC) em que foi isolada |
| `kafka_dlt-original-topic`, `-partition`, `-offset`, `-timestamp`, `-timestamp-type`, `-consumer-group` | Padrão do Spring Kafka (`DeadLetterPublishingRecoverer`) |

- Os headers de exceção do Spring (`kafka_dlt-exception-*`, incluindo stack trace) são **excluídos**: mensagens de
  erro de parsers podem conter trechos do payload (dados pessoais/saldos), e o DLT não deve ampliar essa superfície.
- Produção no DLT: `acks=all`, `enable.idempotence=true`, `max.block.ms` finito e `waitForSendResultTimeout`; a
  publicação é **síncrona**. Se falhar, a mensagem **não é confirmada** (permanece no broker) e é reentregue — validado no
  spike (com o DLT ausente o consumo da partição ficou bloqueado, sem perda, e drenou sozinho ~0,3 s após a criação do tópico).

## 6. Falhas transitórias (nunca vão ao DLT)

Classificação, backoff e pausa detalhados em `research.md` R-08. Resumo: indisponibilidade, throttling, timeout e 5xx do
DynamoDB (`BalanceStoreUnavailableException`) -> backoff exponencial com jitter (500 ms x2 até 30 s, jitter 250 ms,
tentativas ilimitadas), o container é **pausado** entre tentativas e a mensagem permanece no broker.

## 7. Reprocessamento manual do DLT (fora do escopo automático)

```bash
# inspeciona (headers incluídos)
docker compose run --rm --entrypoint rpk redpanda-seed topic consume transacoes-financeiras-processadas.DLT \
  --brokers redpanda:9092 -o start -n 10 -f '%h\n%v\n\n'
# reprocessa após corrigir a causa (o valor volta ao tópico principal)
docker compose run --rm --entrypoint bash redpanda-seed -c \
  "rpk topic consume transacoes-financeiras-processadas.DLT --brokers redpanda:9092 -o start -f '%v\n' -n 1000 \
   | rpk topic produce transacoes-financeiras-processadas --brokers redpanda:9092 -f '%v\n'"
```

Mensagens rejeitadas por `invalid_timestamp` (futuro) só serão aceitas ao reprocessar depois que o relógio alcançar o
instante do evento ou com a tolerância aumentada.
