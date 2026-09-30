# ADR-0002: Modelagem DynamoDB de snapshot por conta

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípios II, IV e VIII; `specs/001-consulta-saldo/research.md` R-03 e R-17; `data-model.md` seção 4

## Contexto

O serviço mantém, por conta, o snapshot da transação de maior precedência `(timestamp em microssegundos, transactionId)` e o
expõe em `GET /balances/{accountId}`. Os eventos chegam por Kafka com entrega at-least-once, sem ordem e sem chave garantida; a
arbitragem de qual evento prevalece precisa ser atômica e feita pelo banco (Constitution II). A carga
assumida (premissa do autor; o enunciado não fixa volume) é de 1.000 eventos/s na ingestão e 500 consultas/s, sobre mais de um milhão
de contas.

Os padrões de acesso são apenas dois, ambos por conta:

| # | Padrão | Operação | Chave |
|---|--------|----------|-------|
| AP1 | Saldo por conta (API) | `GetItem` com `ConsistentRead=true` | `pk=ACCOUNT#<id>`, `sk=BALANCE` |
| AP2 | Aplicar evento (ingestão) | `UpdateItem` condicional (upsert) | idem |

## Decisão

- Tabela única `AccountBalances`, **on-demand** (`PAY_PER_REQUEST`), sem TTL, sem streams e **sem GSI/LSI**.
- **Partition key** `pk` (S) = `ACCOUNT#<accountId em minúsculas>`; **sort key** `sk` (S) = constante `BALANCE`.
- **Um item por conta** com o snapshot vigente: `schemaVersion`, `ownerId`, `accountStatus`, `balanceAmount` (N, `BigDecimal`
  exato), `balanceCurrency`, `accountCreatedAtMicros`, e a chave de precedência `lastTxTsMicros` (N, µs) + `lastTxId` (S).
- A tabela e o item de exemplo do enunciado são criados de forma idempotente por `infra/dynamodb/seed.sh` (`create-table` só se a
  tabela não existir; `put-item` do item de `infra/dynamodb/account-balances.json`).
- **Ledger de transações e GSI por titular não entram na v1**; ficam documentados como evolução, com desenho e gatilho.

### Por que o `sk` é constante

O *key schema* de uma tabela DynamoDB é imutável. Usar `pk`/`sk` genéricos com prefixo permite acrescentar, sem migração, outros
tipos de item na mesma partição da conta (por exemplo `TX#<ts>#<id>` de um ledger). O custo hoje é zero: o item de saldo ocupa
uma única chave `(ACCOUNT#id, BALANCE)`.

### Evolução documentada

| Item | Gatilho | Desenho |
|------|---------|---------|
| Ledger `TX#...` (com TTL) | Requisito de histórico/extrato ou auditoria de eventos aplicados | Duas escritas **independentes e idempotentes**: `PutItem` do `TX#` com `attribute_not_exists(sk)` e o `UpdateItem` do snapshot. **Nunca `TransactWriteItems`**: a rejeição do snapshot por obsolescência abortaria a transação inteira e perderia o registro do ledger |
| GSI por titular (AP3: contas de um titular) | Requisito de consulta por titular | `gsi1pk=OWNER#<ownerId>`, `gsi1sk=ACCOUNT#<id>`, índice esparso com projeção parcial |
| Write sharding / coalescência de escritas | Conta quente com throttling ou muitos eventos obsoletos | Ver `research.md` R-09 e `data-model.md` 4.5 |

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| `PK=accountId` sem prefixo e sem SK | Mais simples, mas fecha a porta para múltiplos tipos de item na partição sem migrar o *key schema* |
| Um item por transação e "último" por `Query` (`ScanIndexForward=false`, `Limit=1`) | Leitura mais cara e escrita sem condição sobre o estado vigente, ou seja, sem arbitragem atômica (proibido pela Constitution II) |
| Ledger `TX#` junto do snapshot na v1 | Dobra as escritas (~2 WCU por evento, cerca de +1.000 WCU/s na carga assumida), concentra o dobro de escrita na partição das contas quentes, não melhora a corretude (que depende só da condição no snapshot) e entrega um histórico que a spec não expõe |
| GSI por titular | Nenhum padrão de acesso o justifica (Constitution VIII, YAGNI); custaria WCU adicional em toda escrita |
| `BatchWriteItem` na ingestão | Não suporta `ConditionExpression`; inaplicável à escrita condicional do snapshot |
| Saldo como `S` (texto) ou como `N` em unidade mínima | `S` preserva escala que o contrato não garante e não é numérico no banco; unidade mínima exigiria conversão nos dois sentidos e regra de casas por moeda. Ver `research.md` R-06 |

## Consequências

- (+) As duas operações são por chave primária: sem índice, sem `Scan`, custo previsível e escrita mínima (1 WCU por evento).
- (+) A arbitragem de precedência acontece no banco com uma única escrita condicional; convergência independente de ordem,
  duplicação e paralelismo.
- (+) Modelo aberto a extensão (ledger, GSI) sem migração do *key schema*.
- (-) Sem histórico de transações e sem consulta por titular na v1; ambos ficam como evolução documentada acima.
- (-) O DynamoDB normaliza zeros à direita de números (`183.10` vira `183.1`): o valor é idêntico e a escala é completada às casas
  da moeda apenas na resposta, sem arredondar (ver ADR de dinheiro e tempo).
- (-) Em produção, PITR e proteção contra exclusão seriam habilitados por IaC, fora do escopo deste repositório (`research.md` R-17).
