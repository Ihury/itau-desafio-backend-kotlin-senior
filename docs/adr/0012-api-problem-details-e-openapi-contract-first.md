# ADR-0012: API com Problem Details e contrato OpenAPI contract-first

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Restrições Técnicas (API) e Princípio VIII; `specs/001-consulta-saldo/research.md` R-12; `contracts/openapi.yaml`; FR-024, FR-025 e FR-026

## Contexto

`GET /balances/{accountId}` precisa de respostas explícitas e distinguíveis para conta existente, inexistente, desabilitada,
identificador malformado, armazenamento indisponível e falha interna, sem nunca vazar pilha, nome de infraestrutura, saldo ou
titular. O contrato deve ser documentado em OpenAPI e não pode divergir do que o serviço realmente responde.

## Decisão

- **Formato de erro**: Problem Details (RFC 9457), `application/problem+json`, com `type`, `title`, `status`, `detail` e
  `instance`. O `type` é uma URN estável e independente de domínio real: `urn:problem-type:consulta-saldo:<slug>`. O `detail` é
  uma mensagem fixa, nunca derivada da mensagem da exceção.
- **Mapeamento** (`ProblemDetailsAdvice`):

  | Situação | Status | `type` (slug) | Observação |
  |----------|--------|---------------|------------|
  | `accountId` não é UUID canônico | 400 | `requisicao-invalida` | o armazenamento não é consultado (FR-024) |
  | conta sem snapshot | 404 | `conta-nao-encontrada` | nunca saldo zerado |
  | snapshot vigente DISABLED | 409 | `conta-desabilitada` | corpo sem saldo, titular nem instante |
  | armazenamento indisponível, lento ou circuit breaker aberto | 503 | `servico-indisponivel` | `Retry-After` |
  | qualquer outra falha | 500 | `erro-interno` | genérico, com o detalhe apenas no log |

- **Só a falha de `AccountId.parse` dentro do controller vira 400** (via exceção local do adapter). `InvalidEventException`
  vinda do caso de uso e `IllegalStateException` de item corrompido são defeitos internos e saem como 500: dado persistido
  inválido não é culpa do chamador nem "conta não encontrada".
- **`Retry-After` fixo**, igual à espera do circuit breaker em OPEN (`balance.circuit-breaker.open-wait`, 10 s por padrão).
- Erros do framework (rota inexistente, método inválido) também saem em Problem Details (`spring.mvc.problemdetails.enabled`).
- `X-Correlation-Id` é aceito se casar `^[A-Za-z0-9._-]{1,64}$`, senão um UUID é gerado; o cabeçalho é devolvido em todas as
  respostas, inclusive nas de erro, e entra no MDC dos logs.
- **Contrato OpenAPI 3.1 estático, contract-first**: `specs/001-consulta-saldo/contracts/openapi.yaml` é a fonte de verdade,
  copiado verbatim para `src/main/resources/static/openapi.yaml` e servido em `GET /openapi.yaml` como `application/yaml`
  (`OpenApiController`; o handler estático padrão o serviria como `application/octet-stream`, pois nem o JDK nem o Spring
  conhecem a extensão `yaml`).
- **Teste anti-drift** (`OpenApiContractTest`): lê o documento do classpath, parseia o YAML e compara com as respostas reais do
  controller: status documentados, media type, `type` e `status` constantes, propriedades obrigatórias e ausência de
  propriedades fora do schema, cabeçalhos declarados, `pattern` de `accountId` e `X-Correlation-Id`, exemplos e a igualdade
  byte a byte da cópia com o contrato em `specs/` quando este existe.
- **Sem springdoc**.

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| springdoc-openapi 3.1.1 | Compatível com o Boot 4.1.0, mas gera o documento a partir de anotações espalhadas nos controllers, embute o `swagger-ui` (superfície extra) e o changelog da 3.1.1 lista oito avisos de segurança; para um serviço de core banking sem requisito de UI é superfície de ataque sem contrapartida |
| Documento só em `docs/` (sem servir) | Não é verificável em runtime; o teste anti-drift precisa do arquivo no classpath |
| `type` como URL de um domínio real | Depende de um domínio que não existe; a URN é estável e não promete uma página |
| `Retry-After` dinâmico (tempo restante do circuito) | O Resilience4j não expõe o tempo restante de forma barata; o valor fixo é um limite superior honesto |
| Mapear `InvalidEventException` como 400 no advice | Faria um item corrompido no banco aparecer como erro do chamador; por isso só o parse do controller é 400 |

## Consequências

- (+) Erros distinguíveis por código HTTP e por `type` estável; nenhuma resposta vaza detalhes internos.
- (+) O contrato não pode divergir em silêncio: qualquer mudança em status, `type`, campos, cabeçalhos ou exemplos quebra o teste.
- (-) O documento não é gerado do código: a mudança de contrato é feita à mão em `specs/` e copiada; o teste de igualdade byte a
  byte a protege quando `specs/` existe (no estágio `test` do Dockerfile, que não copia `specs/`, os demais testes ainda
  comparam o documento do classpath com o comportamento real).
- (-) `Retry-After` pode superestimar a espera real até o circuito tentar fechar.
