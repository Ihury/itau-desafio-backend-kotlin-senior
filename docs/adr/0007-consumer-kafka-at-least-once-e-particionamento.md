# ADR-0007: Consumer Kafka at-least-once e particionamento

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípios II e III; `specs/001-consulta-saldo/research.md` R-07 e R-09; `contracts/kafka-events.md` seções 1 e 2

## Contexto

O tópico `transacoes-financeiras-processadas` entrega mensagens **sem chave e sem ordem por conta**, com possíveis duplicatas. O
serviço nunca pode perder uma mensagem válida, e a corretude não pode depender de ordem, de número de partições nem de quantas
instâncias consomem (a arbitragem é do banco, ADR-0003). O offset só pode avançar depois que o efeito foi persistido.

## Decisão

- **Listener por registro** (`TransactionEventListener`, `@KafkaListener` sobre `ConsumerRecord<ByteArray?, ByteArray?>`), sem
  `Acknowledgment` na assinatura: o commit é responsabilidade do container.
- **Bytes verbatim**: `ByteArrayDeserializer` para chave e valor. Nunca lança e preserva o conteúdo original (inclusive binário ou
  UTF-8 inválido) para o DLT; a validação é do parser estrito (ADR-0006).
- **Entrega at-least-once**: `enable.auto.commit=false` e `AckMode=BATCH`: o container confirma os offsets do poll somente depois
  que o listener retornou para todos os registros. Uma exceção do listener nunca confirma o offset do registro que falhou.
- **Sem suposição de ordem**: nenhuma lógica depende da ordem das mensagens; duplicatas e reordenação são resolvidas pela escrita
  condicional. Por isso é seguro aumentar partições.
- **Partições**: 12 no tópico principal e 3 no DLT (decisão proposta, sujeita a validação com o cliente), RF 1 local e 3 em
  produção, DLT com retenção de 14 dias. 1.000 eventos/s divididos por ~150 eventos/s por thread (escrita ~5-7 ms) dá ~7
  consumidores; 12 dá folga sem coordenação excessiva.
- **`CooperativeStickyAssignor`**: rebalanceamento incremental, sem parar todo o grupo a cada mudança de membros.
- **Concorrência e limites**: `concurrency=4` por instância (a soma entre instâncias deve ser <= partições), `max.poll.records=100`
  e `max.poll.interval.ms=300000`. Invariante verificada por teste: `max.poll.records x DYNAMODB_WRITE_CALL_TIMEOUT <
  max.poll.interval.ms` (100 x 2 s = 200 s < 300 s), para o pior caso de um poll não provocar rebalance.
- **Parada**: `immediate-stop=true` com encerramento gracioso; o container para após o registro atual e o restante do poll é
  reentregue (é idempotente).
- **Error handler sem descarte desde o primeiro commit com consumer**: na US2, `FailSafeErrorHandlerConfig` (backoff exponencial
  de 500 ms a 30 s, sem limite de tentativas, recoverer que nunca confirma o offset, todas as exceções retentáveis): mensagem
  inválida ou falha transitória ficava retida e reentregue. Na US4 ele foi substituído (mesmo bean `kafkaErrorHandler`, um único
  `CommonErrorHandler`) pelo `DeadLetterConfig`, que classifica por exceção: inválida -> DLT imediato; transitória -> o mesmo
  backoff ilimitado e NUNCA DLT; não classificada -> 3 entregas e DLT. A pausa do container entra na US5; ver ADR-0008.

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| `AckMode=RECORD` (um commit por mensagem) | Custo alto a 1.000 msg/s sem ganho: reprocessar até 100 registros é idempotente |
| `AckMode=MANUAL` | Complexidade sem benefício; o commit em lote depois do retorno do listener já garante persistir antes de confirmar |
| `ErrorHandlingDeserializer(JsonDeserializer)` | Perde os motivos finos de rejeição e os bytes originais (ADR-0006) |
| `BatchListener` com coalescência por conta | Como os eventos não têm chave e a conta é aleatória, dois eventos da mesma conta raramente caem no mesmo lote; a coalescência complicaria a classificação de erro parcial e o desfecho único por mensagem |
| Concorrência igual ao número de partições | Mais threads que a instância precisa; o valor é configurável |
| Protocolo de grupo do KIP-848 | Fora do escopo |

## Consequências

- (+) Nenhuma mensagem é perdida por desenho: só o container confirma offsets, e só depois do retorno do listener.
- (+) Escalar consumidores ou partições não afeta a corretude.
- (-) Uma queda no meio de um poll reprocessa até 100 registros; o efeito é idempotente, mas consome capacidade de escrita
  (escritas obsoletas e duplicadas, ADR-0003).
- (-) Até a US4 uma mensagem inválida fica retida e reentregue com backoff, bloqueando a partição, em vez de ir ao DLT.
