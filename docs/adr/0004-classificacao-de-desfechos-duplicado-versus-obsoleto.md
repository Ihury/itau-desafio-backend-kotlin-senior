# ADR-0004: Classificação de desfechos: duplicado versus obsoleto e anomalia

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípios II e III; `specs/001-consulta-saldo/research.md` R-04; `data-model.md` seções 4.4 e 6; FR-005, FR-006, FR-008 e a seção Edge Cases da spec; ADR-0003

## Contexto

Com a escrita condicional atômica (ADR-0003), um evento que não supera o snapshot vigente falha na condição. Isso não é erro,
mas o operador precisa distinguir três situações para observar o sistema (FR-031: exatamente um desfecho por evento): o evento
já foi aplicado (duplicado), o vigente é mais novo (obsoleto) ou a origem reenviou a mesma chave com conteúdo diferente
(anomalia). A classificação não pode custar uma leitura extra por evento nem reintroduzir read-modify-write.

## Decisão

- `ReturnValuesOnConditionCheckFailure=ALL_OLD` faz o DynamoDB devolver o **item vigente na própria
  `ConditionalCheckFailedException`** (`exception.item()`). O `DynamoDbBalanceSnapshotWriter` classifica por ele, sem leitura extra:
  - mesma `(lastTxTsMicros, lastTxId)` do evento: **`Duplicate`**. Se `ownerId`, `accountStatus`, `balanceCurrency` ou
    `balanceAmount` divergirem, `Duplicate(conflicting = true)`; o saldo é comparado **numericamente** (`compareTo`), porque o
    DynamoDB pode normalizar a escala (`183.10` vira `183.1`) e `100.00` e `100` são o mesmo valor;
  - precedência do vigente maior: **`Obsolete`**;
  - o vigente inferior ao evento com condição falsa é uma **contradição** (leitura inconsistente do endpoint ou item
    gravado fora do padrão), não indisponibilidade: `IllegalStateException` sem valores, tratada pelo consumer como
    falha não classificada (3 entregas e DLT `unprocessable_event`, com log e métrica). Decisão revisada na US4: a versão
    inicial lançava `BalanceStoreUnavailableException`, o que retentaria para sempre (backoff ilimitado) e bloquearia a
    partição se a causa fosse permanente. Continua transitório apenas o `GetItem` de fallback sem item (a próxima
    tentativa cria a conta).
- **Duplicado** é o evento cuja chave é **igual** à do vigente. **Obsoleto** é qualquer evento de precedência inferior,
  **inclusive a reentrega de uma transação que já foi superada**: sem ledger, ela é indistinguível de qualquer evento antigo, e
  ambos são "sem efeito e contabilizados", que é o que FR-005 e FR-006 exigem.
- **Anomalia `conflicting_duplicate`**: mesma chave `(timestamp, transactionId)` com conteúdo divergente. O desfecho continua
  sendo `duplicate` (contado uma única vez), o contador `balance.events.anomalies{type=conflicting_duplicate}` é incrementado e
  um log WARN registra apenas `accountId` e `transactionId` (nunca saldo nem titular). **Prevalece o primeiro evento aplicado**:
  a anomalia é defeito da origem e fica **fora da garantia de convergência de FR-008** (a garantia vale para eventos cujo
  conteúdo é função da chave), como registrado na spec (Edge Cases). Por isso a propriedade de convergência deriva o conteúdo
  da chave e testa a anomalia à parte.
- Fallback raro: se a exceção vier sem o item (`hasItem() == false`), uma única `GetItem` fortemente consistente o obtém; item
  ausente também nesse caminho é falha transitória (reentrega). Coberto por teste.
- Um item vigente ilegível (atributo ausente ou numérico inválido) é falha interna (`IllegalStateException` sem valores na
  mensagem): o consumer a trata como não classificada (3 entregas e DLT), nunca "classifica no escuro".

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| `GetItem` após cada condição falsa | Uma ida ao banco a mais por evento obsoleto, exatamente o volume que a condição atômica poupa; e a leitura poderia já estar desatualizada |
| Ledger de transações aplicadas para distinguir reentrega antiga de obsoleto | Uma escrita extra por evento, um segundo item a manter consistente e nenhum padrão de acesso que o exija (FR-005/FR-006 tratam ambos como sem efeito) |
| Tratar toda falha de condição como `Obsolete` (Fase 4) | Perde a métrica de duplicidade e esconde a anomalia de conteúdo divergente |
| Aplicar o último dos duplicados divergentes | Contradiz a precedência estrita da condição (o banco não substitui um item de mesma chave) e tornaria o resultado dependente da ordem |
| Rejeitar a anomalia no DLT | O evento não é inválido; a origem é que reenviou a mesma chave. Alarmar por métrica e log, sem isolar |

## Consequências

- (+) Nenhuma leitura extra no caminho normal; classificação exata e testável no contrato (fake x DynamoDB Local).
- (+) A anomalia é observável por métrica dedicada, sem alterar o desfecho contado.
- (-) A reentrega de transação já superada é contada como `obsolete`, não como `duplicate` (limite conhecido, sem ledger).
- (-) A garantia de convergência não cobre chaves repetidas com conteúdo divergente: prevalece o primeiro.
