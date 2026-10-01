# ADR-0001: Arquitetura hexagonal por bounded context e verificação Konsist ampliada

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípio I e VIII; `specs/001-consulta-saldo/research.md` R-01; `plan.md` (Complexity Tracking)

## Contexto

O starter-kit traz um único contexto de exemplo, `hello` (saudações com template no DynamoDB e um `@KafkaListener` de um tópico
`greeting-templates`), e um teste Konsist (`HexagonalArchitectureTest`) que verifica **apenas** o pacote `hello`. A Constitution
(Princípio I) exige que a direção de dependências seja verificada por teste de arquitetura cobrindo **todos** os pacotes de
negócio: um contexto novo, como o de consulta de saldo, ficaria fora da verificação se o teste continuasse amarrado a `hello`.

Além disso, manter um endpoint `/hello`, um consumer de tópico alheio e uma tabela sem uso num serviço de core banking é ruído e
risco operacional (listener que tenta conectar ao broker durante os testes, superfície de ataque sem função de negócio).

## Decisão

1. Todo o código de negócio vive em contextos delimitados, subpacotes diretos de `br.com.itau.challenge`. O contexto desta feature
   é `balance`, com a estrutura `domain/{model,exception}`, `port/{input,output}`, `application`,
   `adapter/{input/web,input/kafka,output/dynamodb,output/metrics}` e `config`.
2. O pacote `config` é o *composition root* (propriedades, `Clock`, registry do circuit breaker, beans de serviço). Fica fora das
   quatro camadas e **ninguém** depende dele.
3. O exemplo `hello` é **removido** (fontes, testes, seeds, `http/hello.http`, propriedades do exemplo).
4. O teste `ArchitectureTest` substitui `HexagonalArchitectureTest` e usa `Konsist.scopeFromProduction()`. Os contextos são
   **descobertos** (subpacotes diretos do pacote raiz), então um contexto futuro entra na verificação sem alterar o teste. Regras:
   - **(0)** direção das camadas por contexto (`assertArchitecture`): `domain` não depende de nada; `port` não depende de
     `application` nem de `adapter`; `application` não depende de `adapter`. Camadas ainda vazias são toleradas;
   - **(a)** imports de `domain` limitados a `kotlin.*`, `java.*` e o próprio domínio (nada de Spring, AWS, Kafka, Jackson,
     Micrometer, Resilience4j);
   - **(b)** imports de `application` limitados a domain, port, `org.springframework.stereotype.Service`, `org.slf4j`, `kotlin.*`
     e `java.*`;
   - **(c)** cada tecnologia de adapter (`input.web`, `input.kafka`, `output.dynamodb`, `output.metrics`) não importa as demais;
   - **(d)** nenhum arquivo fora de `config` importa `..config..`;
   - **(e)** guarda: todo subpacote direto de um contexto pertence a `{domain, port, application, adapter, config}`.
5. A robustez das regras foi demonstrada por mutação: para cada regra, um import proibido temporário no código de `hello` (ainda
   presente na época) fez o teste correspondente falhar, e a mutação foi revertida. O teste, portanto, não é vacuoso.

### Exceção controlada: `@Service` e `org.slf4j` na camada `application`

Decisão do orquestrador do projeto, sem emenda na Constitution: o starter-kit anota seus serviços de aplicação com `@Service` e
usa SLF4J. O Princípio I é lido como **direção de dependência entre camadas** (`application` depende só de `domain` e `port`).
Os **únicos** imports externos permitidos na `application` são `org.springframework.stereotype.Service` e `org.slf4j`, garantidos
pela lista de permissão da regra (b). Qualquer outro import de framework na `application` (por exemplo `@Value`, Micrometer,
AWS SDK) quebra o teste. Métricas e relógio entram por *ports* (`OutcomeMetrics`, `ConsumerFailureMetrics`, `IngestMetrics`) e `java.time.Clock`, então a camada continua
livre de Micrometer.

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| Manter `hello` ao lado de `balance` | Contexto extra a proteger e explicar; listener de tópico alheio; confunde o avaliador |
| `application` 100% sem Spring, com `@Bean` no `config` | Mais puro, mas diverge da convenção do starter sem ganho verificável; a whitelist Konsist já dá a garantia que importa |
| ArchUnit no lugar do Konsist | Mais uma dependência; o Konsist já é o padrão do starter e atende às regras |
| Um único `Layer` global (`..domain..`) em vez de regras por contexto | Não impede dependência entre contextos nem dá mensagem de violação por contexto |

## Consequências

- (+) Todo contexto atual e futuro é verificado automaticamente; violações apontam arquivo e import.
- (+) Fronteiras de camada e de tecnologia de adapter são executáveis, não convenção de revisão.
- (+) Remover `hello` elimina o listener que tentava conectar ao broker no contexto de teste.
- (-) `@Service` na `application` é uma exceção à leitura mais estrita do Princípio I; fica explícita e limitada pela whitelist.
- (-) A regra (0) usa o `assertArchitecture` do Konsist com camadas montadas dinamicamente (camadas vazias não são declaradas);
  a existência de ao menos um contexto de negócio com `domain` é exigida assim que o primeiro código de negócio entra
  (unidade de domínio do plano).
