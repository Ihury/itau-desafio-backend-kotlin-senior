# ADR-0014: Estratégia de testes e evidência de corretude

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípios II, III e VI; `specs/001-consulta-saldo/research.md` R-14; `data-model.md` seção 7; SC-003, SC-004, SC-005

## Contexto

O valor do serviço está na **corretude sob desordem, duplicidade e concorrência**. Testes por exemplo, escolhidos por quem
escreveu o código, tendem a confirmar o caso que o autor imaginou. A Constitution VI (não negociável) exige TDD e prova de que
os testes detectam defeitos, não apenas que passam.

## Decisão

1. **TDD**: teste vermelho antes do código, em commits pequenos. Os testes de integração escritos depois da implementação
   (caracterização) registram **evidência de vermelho** (Regra 7 das tarefas): mutação temporária e descartável na
   `ConditionExpression` ou execução contra um dublê defeituoso, anotada na descrição do commit.
2. **Propriedade** (`kotest-property` 6.2.5, `ConvergencePropertyTest`): `seed` fixa `20260929`, 1.000 iterações, listas de 1 a 30
   eventos de uma ou duas contas. Timestamps num intervalo de 4 microssegundos (empates frequentes), seis `transactionId`
   fixos com pares em que `UUID.compareTo` diverge da ordem textual, ids em maiúsculas, `ENABLED`/`DISABLED` e
   `APPROVED`/`DECLINED`. O **conteúdo do evento é derivado da chave** `(conta, timestamp, transactionId)`. Cada caso entrega o
   mesmo conjunto em 8 ordens (dada, inversa, crescente, decrescente, 4 embaralhadas) mais uma entrega com tudo duplicado, e
   confere o snapshot final (oráculo por `String.compareTo`, sem `UUID.compareTo`) e o desfecho de **cada** evento contra um
   modelo de referência (`Applied`/`Duplicate`/`Obsolete`). A anomalia `conflicting_duplicate` fica de fora por definição e é
   testada à parte (ADR-0004).
3. **Meta-teste**: a MESMA propriedade é executada contra o `NaiveLastWriteWinsStore` ("o último a chegar vence") e **tem de
   falhar**. Contraexemplos encolhidos pelo kotest (de 17 eventos):
   - propriedade completa: um único evento `(ts+0, 00000000-0000-4000-8000-000000000001)` entregue duas vezes; esperado
     `Duplicate(conflicting=false)`, obtido `Applied`;
   - só a convergência do snapshot final: dois eventos `[(ts+0, tx0), (ts+3, tx0)]` entregues na ordem `ts+3`, `ts+0`; o store
     ingênuo termina no de `ts+0` em vez do de `ts+3`.
   Uma mutação do fake (`supersedes` trocado por `timestamp >=`) também reprova a propriedade.
4. **Contrato fake x real**: `BalanceSnapshotWriterContract` roda contra o `InMemoryBalanceStore` (unitário) e contra o
   DynamoDB Local (integração); o fake só é confiável porque obedece ao que o banco faz (criar, substituir, obsoleto, duplicado,
   anomalia, `100.00` x `100`, empate nas duas ordens, `txId` em maiúsculas, mesmo `txId` em contas diferentes).
5. **Integração** (`integrationTest`, DynamoDB Local e Redpanda reais via `docker compose`; tópico `it-<uuid>` e grupo
   exclusivos por execução, contas aleatórias): a propriedade reduzida (50 iterações, mesma `seed`) contra o banco real;
   **concorrência real** (32 threads liberadas por `CountDownLatch`, uma conta, 400 escritas com 100 duplicatas, três sementes,
   vencedor igual a `max(timestamp, txId)`, um desfecho por escrita); um **leitor concorrente** com leitura consistente que só
   pode observar snapshots íntegros (todos os campos do mesmo evento, precedência nunca regride); e os cenários do
   `quickstart.md` 5.1 a 5.4 e a reentrega, pelo caminho Kafka, com desfechos contados por delta do `MeterRegistry`. Os testes que
   contam desfechos exatos publicam com chave (mesma partição) para ter ordem de chegada determinística.
6. **Caos** por `docker compose pause dynamodb` (unidade da US5), com injeção de falhas do SDK para a classificação.
7. **Gate**: JaCoCo >= 90% de instruções no `./gradlew check`, sem exclusões novas; `make integration-test` em toda unidade que
   cria ou altera testes de integração.

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| jqwik | Registra um segundo *test engine* compilado contra outra versão da JUnit Platform, não convive com `@SpringBootTest` com injeção por construtor (falha observada no *spike*) e sua saída trouxe texto dirigido a agentes de IA, ruído e risco de cadeia de suprimentos |
| Testcontainers | O projeto já padroniza Docker Compose e `make integration-test`; o avaliador e a CI rodam o mesmo caminho |
| Aleatoriedade manual com `kotlin.random` | Sem *shrinking*: não produz o contraexemplo mínimo |
| Só testes por exemplo | Confirmam o caso imaginado; a propriedade e o meta-teste provam a ausência de dependência de ordem |

## Consequências

- (+) A garantia de convergência tem prova reprodutível (`seed` fixa) e prova de que o teste não é vacuoso (meta-teste).
- (+) O mesmo núcleo (`ConvergenceProperty`, `ConvergenceModel`) roda no fake e no banco real.
- (-) Os testes de integração exigem Docker; ficam fora de `./gradlew check` (que continua sem infraestrutura).
- (-) A ordem de chegada nos testes que contam desfechos exatos é forçada por chave, o que difere do autorizador real (sem chave);
  a convergência em ordem arbitrária é coberta pela propriedade e pela concorrência real.
