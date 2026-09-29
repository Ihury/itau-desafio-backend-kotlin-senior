# ADR-0003: Precedência determinística e escrita condicional atômica

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípios II e III; `specs/001-consulta-saldo/research.md` R-04; `data-model.md` seções 2.1 e 4.4; FR-003 a FR-008

## Contexto

Os eventos chegam por Kafka com entrega at-least-once, **sem ordem, sem chave e possivelmente em paralelo** (várias threads e
várias instâncias). Para uma mesma conta, o snapshot deve convergir para o evento de maior precedência, seja qual for a ordem, a
duplicação ou a concorrência das entregas. Ordenar por horário de processamento é proibido (FR-003): o horário do relógio local
não diz qual evento aconteceu depois.

## Decisão

- **Precedência** = `(transaction.timestamp em µs, transaction.id)`: ordem total por `timestamp` numérico e, no empate, por
  `transactionId` na **comparação lexicográfica da string canônica em minúsculas**. Um evento supersede o vigente se e somente se
  tem precedência estritamente maior; igualdade total é o mesmo evento (duplicado).
- **Uma única `UpdateItem` por evento**, executada pelo próprio banco, com

  ```text
  ConditionExpression: attribute_not_exists(pk)
                       OR lastTxTsMicros < :ts
                       OR (lastTxTsMicros = :ts AND lastTxId < :tx)
  ReturnValuesOnConditionCheckFailure: ALL_OLD
  ```

  Condição verdadeira: os oito atributos do snapshot mudam juntos (item único, atômico: a consulta nunca mistura campos de
  eventos diferentes). `ConditionalCheckFailedException` **não é erro**: o vigente tem precedência maior ou igual e o evento é
  descartado como obsoleto. `ALL_OLD` devolve o item vigente na própria exceção, sem leitura extra, o que permite separar
  duplicado de obsoleto (refinamento na convergência, ADR-0004).
- **Sem read-modify-write, sem lock local, sem `BatchWriteItem` nem `TransactWriteItems`.** O banco é o árbitro entre threads e
  instâncias; nada em memória decide precedência.
- `balanceAmount` é gravado como `N` a partir de `BigDecimal.toPlainString()` (ADR-0005).

### Armadilhas tratadas

- `java.util.UUID.compareTo` compara `long`s **com sinal** e diverge da ordem textual; o DynamoDB compara strings por bytes UTF-8.
  Por isso o desempate usa a string canônica nos dois lados. Maiúsculas ordenariam antes das minúsculas: a normalização para
  minúsculas é obrigatória (spike: 2.000 UUIDs aleatórios, 0 divergências entre `String.compareTo` e a condição do banco).
- `UUID.fromString("1-1-1-1-1")` é aceito pelo JDK; o parse do identificador é estrito, por regex (ADR-0006).
- **Custo**: uma escrita com condição falsa **ainda consome 1 WCU**; eventos obsoletos custam capacidade. Mitigação futura:
  coalescência por conta no lote de consumo (`research.md` R-09), sem trocar a garantia do banco.

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| Ler o item, comparar e gravar (read-modify-write) | Janela entre a leitura e a escrita: dois consumidores podem sobrescrever o mais novo com o mais antigo. Proibido pela Constitution II |
| Versão otimista `version = version + 1` com retry | Continua sendo leitura seguida de escrita e acrescenta retentativas; a precedência não vem de um contador, vem do evento |
| Lock local ou distribuído por conta | Não vale entre instâncias, cria ponto de contenção e estado a recuperar |
| `TransactWriteItems` | A obsolescência aborta a transação inteira e o custo por escrita é maior; não há segundo item a proteger na v1 |
| Ordenar por horário de processamento | Proibido (FR-003): não representa a ordem dos fatos |
| `timestamp` como `S` | A comparação lexicográfica exigiria zero-padding; `N` compara numericamente |

## Consequências

- (+) Convergência independente de ordem, duplicação e paralelismo, provada por propriedade sobre o fake e por concorrência real
  contra o DynamoDB Local nas unidades seguintes.
- (+) Uma ida ao banco por evento, sem leitura extra para classificar.
- (-) Escritas obsoletas consomem capacidade de escrita; observável pela razão `obsolete` de `balance.events`.
- (-) O contrato de teste (`BalanceSnapshotWriterContract`) precisa rodar contra o fake **e** contra o DynamoDB Local, para que o
  fake só seja confiável porque obedece ao que o banco real faz.
