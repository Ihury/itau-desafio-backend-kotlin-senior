# ADR-0006: Validação estrita e catálogo de motivos de rejeição

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípios III e IV; `specs/001-consulta-saldo/contracts/kafka-events.md` seções 3 e 4; `research.md` R-06 e R-07; FR-009, FR-012, FR-016

## Contexto

O formato do evento é imposto pelo cliente e o tópico de entrada não é confiável: chegam JSON malformado, binário, números como
string, `1E999999999`, UUIDs em formatos que o JDK aceita mas não são canônicos, timestamps em segundos ou milissegundos e datas
no futuro. Dados inválidos nunca podem alcançar a persistência (Princípio IV), e cada mensagem rejeitada precisa de **um único
motivo enumerado e estável** (FR-016) para virar métrica, header do DLT e alerta.

## Decisão

- **Parser próprio no adapter `input/kafka`** (`TransactionEventParser`), sobre a **árvore JSON** (`JsonNode`), sem DTO tipado e
  sem coerção silenciosa. Um DTO com `ACCEPT_FLOAT_AS_INT` truncaria `1.5` para `1`, e `"97.07"` como string passaria como número.
- `JsonMapper` **privado** do parser (não é `@Bean`: um bean customizado desativaria o `JsonMapper` auto-configurado do Spring MVC),
  com `USE_BIG_DECIMAL_FOR_FLOATS` (dinheiro nunca passa por `double`), `STRICT_DUPLICATE_DETECTION`, falha em tokens finais e
  `StreamReadConstraints` (número <= 1.000 caracteres, aninhamento <= 500). Payload limitado a 64 KiB e decodificação UTF-8
  estrita, sobre os bytes verbatim do registro.
- **Ordem fixa de passos; a primeira falha determina o motivo**: (1) payload e estrutura; (2) presença dos 12 campos; (3) valores,
  na ordem `transaction.id`, `type`, `amount`, `currency`, `status`, `timestamp`, `account.id`, `owner`, `created_at`, `status`,
  `balance.amount`, `balance.currency`; (4) tolerância de futuro em runtime, na camada `application` (precisa de `Clock`).
- **Catálogo**: 7 motivos de validação (`malformed_payload`, `missing_field`, `invalid_identifier`, `invalid_value`,
  `invalid_currency`, `invalid_timestamp`, `unknown_domain_value`) mais `unprocessable_event` (falha interna não classificada).
  `InvalidEventException(reason, detail?)`: `detail` é o **caminho do campo** (`transaction.currency`), ausente em
  `malformed_payload` e `unprocessable_event`, e nunca recebe valores do payload. A exceção de domínio não encadeia a causa do
  parser (mensagens de parsers podem citar trechos do payload: dado pessoal e saldo).
- **Limites de plausibilidade**: `transaction.timestamp` >= `2000-01-01T00:00:00Z` (detecta unidade em s/ms; entra na precedência)
  e `account.created_at` >= `1900-01-01T00:00:00Z` (contas anteriores a 2000 são legítimas e não podem ir ao DLT; não entra na
  precedência), ambos configuráveis (`BALANCE_MIN_EVENT_TIMESTAMP`, `BALANCE_MIN_ACCOUNT_CREATED_AT`). Limite superior =
  relógio + tolerância de 5 minutos (`BALANCE_FUTURE_TOLERANCE`), verificado na `application`.
- **Precisão e escala** de valores monetários impostas no domínio (`Money`, `Transaction`): precisão <= 38 dígitos, escala
  positiva <= 38, escala negativa (`1E+3`) expandida só se `precisão - escala <= 38`, de modo que `1E999999999` é rejeitado sem
  materializar o expoente.
- **Sem `ErrorHandlingDeserializer`**: o consumer usa `ByteArrayDeserializer`, que nunca lança e preserva os bytes originais para o
  DLT (com `StringDeserializer` bytes inválidos virariam `U+FFFD`). A categorização fina em sete motivos exige o parser próprio de
  qualquer forma; delegar ao `ErrorHandlingDeserializer(JsonDeserializer)` entregaria só uma `DeserializationException` genérica.

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| DTO Jackson tipado com `BigDecimal` | Coerção silenciosa (`"10.00"`, `1.5` -> `1`) e erros sem categoria |
| `ErrorHandlingDeserializer(JsonDeserializer)` | Perde os motivos finos e os bytes originais no DLT |
| `UUID.fromString` para identificadores | Aceita `1-1-1-1-1` e outros formatos não canônicos; o parse é por regex estrita 8-4-4-4-12 |
| Um único limite inferior de timestamp (2000-01-01) | Enviaria ao DLT contas legítimas abertas antes de 2000 |
| Acumular todos os erros da mensagem | Uma mensagem, um motivo: o desfecho precisa ser exclusivo para reconciliar as métricas (FR-031) |

## Consequências

- (+) Nenhum valor inválido chega ao banco; cada rejeição tem motivo enumerado, campo e nenhum dado sensível.
- (+) A validação é testada linha a linha por motivo (`TransactionEventParserTest`), inclusive a ordem e a privacidade.
- (-) O parser precisa acompanhar o schema do evento; campos novos e desconhecidos são ignorados por desenho (compatibilidade
  futura), então uma mudança de significado de um campo conhecido só é detectada pelo tipo e pelos limites.
