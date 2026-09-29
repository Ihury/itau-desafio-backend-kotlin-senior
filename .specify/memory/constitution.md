# Consulta de Saldo (Itaú Core Banking) Constitution

## Core Principles

### I. Arquitetura Hexagonal Verificada (NÃO NEGOCIÁVEL)

- `domain` DEVE ser Kotlin puro: nenhuma dependência de Spring, AWS SDK, Kafka, Jackson ou
  qualquer biblioteca de infraestrutura.
- `application` DEVE depender apenas de `domain` e `port`; `port` NÃO DEVE depender de
  `application` nem de `adapter`.
- Adapters DEVEM ser isolados por tecnologia (`input/web`, `input/kafka`, `output/dynamodb`).
  DTOs de transporte e modelos de persistência NUNCA cruzam para `domain` ou `application`.
- A direção de dependências DEVE ser verificada por teste de arquitetura (Konsist) cobrindo
  **todos** os pacotes de negócio da aplicação — nenhum contexto pode ficar fora do escopo.

**Rationale**: aderência à arquitetura do starter-kit é critério de avaliação e é o que permite
trocar broker/banco e testar regras de negócio sem infraestrutura.

### II. Corretude sob Concorrência e Desordem (NÃO NEGOCIÁVEL)

- Toda mutação de estado persistido DEVE ser uma escrita condicional atômica executada pelo
  banco. Read-modify-write na aplicação (ler, comparar em memória, gravar) é PROIBIDO.
- A precedência entre eventos DEVE ser determinística e baseada em dados do próprio evento
  (tempo do evento + identificador para desempate). Horário de processamento NUNCA decide
  qual estado prevalece.
- Locks em memória (`synchronized`, `Mutex`, locks locais) NÃO DEVEM ser usados como
  mecanismo de corretude: a aplicação roda com múltiplas instâncias e múltiplas threads.
- NENHUMA garantia de ordenação do broker pode ser assumida (chave de partição, número de
  partições e rebalances estão fora do nosso controle).
- Evento mais antigo que o estado persistido é um desfecho esperado: DEVE ser descartado sem
  erro e contabilizado em métrica.

**Rationale**: em alto volume, dois eventos da mesma conta são processados concorrentemente ou
fora de ordem; somente o banco pode arbitrar isso de forma atômica entre instâncias.

### III. Idempotência e Entrega At-Least-Once

- Todo consumer DEVE produzir o mesmo estado final diante de reentrega, duplicatas e
  reprocessamento após rebalance ou restart.
- Offsets DEVEM ser confirmados somente após a persistência bem-sucedida; auto-commit é
  PROIBIDO.
- Erros DEVEM ser classificados explicitamente: **transitórios** (retry com backoff) ou
  **permanentes** (payload inválido, desserialização) → Dead Letter Topic com o motivo nos
  headers.
- Uma mensagem inválida NUNCA bloqueia a partição e NUNCA é descartada silenciosamente.

**Rationale**: Kafka entrega at-least-once; exactly-once de ponta a ponta com DynamoDB não
existe, então a idempotência tem que estar no handler e na escrita.

### IV. Integridade de Dados Financeiros

- Valores monetários DEVEM ser `BigDecimal` ponta a ponta — desserialização, domínio,
  persistência e resposta. `Double`/`Float` são PROIBIDOS para dinheiro em qualquer camada.
- Nenhuma conversão com perda silenciosa (arredondamento, truncamento de escala ou de
  precisão de timestamp) é permitida.
- Moedas DEVEM ser códigos ISO 4217 válidos; timestamps DEVEM preservar a precisão de
  microssegundos da origem; datas expostas DEVEM seguir ISO 8601 com offset.
- Validação de formato ocorre na borda (adapter); invariantes de negócio são garantidas no
  domínio. Dados inválidos NUNCA alcançam a persistência.

**Rationale**: saldo errado é pior que indisponibilidade; erros de ponto flutuante e de
precisão são defeitos silenciosos e cumulativos.

### V. Resiliência com Limites Explícitos

- Toda chamada remota DEVE ter timeouts explícitos (conexão, tentativa e total).
- Cada chamada remota DEVE ter exatamente UMA camada de retry (ex.: a do AWS SDK *ou* a da
  aplicação, nunca ambas), com backoff exponencial, jitter e limite de tentativas.
- Com dependência indisponível, a API DEVE falhar rápido (circuit breaker, 503 +
  `Retry-After`); o consumer DEVE aplicar backpressure (backoff/pausa), mantendo as mensagens no
  broker em vez de descartá-las ou enviá-las à DLT.
- Degradação NUNCA pode resultar em dado incorreto: é preferível responder erro a responder
  saldo errado ou desatualizado sem sinalização.

**Rationale**: retries empilhados causam retry storms e amplificam incidentes; o broker já é o
buffer natural da ingestão.

### VI. Test-First e Evidência de Corretude (NÃO NEGOCIÁVEL)

- TDD: o teste DEVE existir e falhar antes da implementação correspondente.
- Cenários obrigatórios: mensagem duplicada, fora de ordem, empate de timestamp, payload
  inválido, conta inexistente, dependência indisponível e escrita concorrente na mesma conta.
- A convergência DEVE ser provada por teste de propriedade: qualquer permutação e duplicação
  de um conjunto de eventos resulta no mesmo estado final.
- A suíte unitária (`test`) NÃO DEVE depender de infraestrutura externa; testes de integração
  (`integrationTest`) DEVEM rodar contra DynamoDB Local e Redpanda reais.
- O gate de cobertura (JaCoCo ≥ 90% de instruções) NÃO PODE ser reduzido nem contornado com
  exclusões não justificadas.

**Rationale**: concorrência e desordem não se validam com happy path; a evidência precisa ser
automatizada e reproduzível pelo avaliador.

### VII. Observabilidade como Requisito

- Logs DEVEM ser estruturados (JSON) com `accountId`, `transactionId` e trace id em contexto.
  Dados pessoais e valores de saldo NÃO DEVEM ser logados.
- Todo desfecho de processamento DEVE gerar métrica (processado, obsoleto, duplicado,
  inválido por motivo, enviado à DLT), assim como latências (p50/p99) de API e de escrita.
- Health checks DEVEM distinguir liveness de readiness e refletir as dependências críticas.
- Nenhum `catch` pode engolir exceção sem log e métrica correspondentes.

**Rationale**: em missão crítica, o que não é medido não é operável; é também critério
explícito de production readiness.

### VIII. Simplicidade e Decisões Registradas

- YAGNI: nenhum componente (índice, cache, tabela, biblioteca, camada) é adicionado sem um
  requisito ou padrão de acesso que o justifique.
- Toda decisão arquitetural relevante DEVE ser registrada como ADR em `docs/adr/`, com
  contexto, alternativas consideradas e trade-offs.
- O que não for implementado por limite de tempo DEVE ser documentado com sua motivação e o
  desenho proposto.

**Rationale**: justificar o que *não* foi feito demonstra tanto critério quanto o que foi feito.

## Restrições Técnicas e de Segurança

- **Stack base** (herdada do starter-kit): Kotlin 2.3, Java 21, Spring Boot 4.1
  (Spring Framework 7), Jackson 3, AWS SDK for Java v2, Spring Kafka, JUnit 5, Konsist, JaCoCo.
  Troca de componente ou de versão major exige ADR.
- **Dependências novas** DEVEM ter compatibilidade comprovada com Spring Boot 4 e versão fixa.
  Imagens Docker DEVEM usar tag fixa (nunca `latest`).
- **Configuração** segue 12-factor: variáveis de ambiente com defaults locais; nenhum segredo
  no código. Fora do ambiente local, credenciais AWS vêm da default credentials provider chain.
- **Container**: processo não-root, heap relativa à memória do container, graceful shutdown e
  healthcheck.
- **API**: entradas validadas (ex.: `accountId` como UUID), erros no formato Problem Details
  (RFC 9457) e contrato documentado em OpenAPI.
- O enunciado original do desafio NÃO DEVE ser versionado no repositório.

## Fluxo de Desenvolvimento e Quality Gates

- **Spec-Driven Development (Spec Kit)**: constitution → specify → clarify → plan → tasks →
  analyze → implement. Os artefatos (`specs/`, `.specify/memory/`, `docs/adr/`) são
  versionados como evidência da metodologia.
- **Uso de IA**: a IA atua como executora sob especificação. Toda saída é revisada, e as
  decisões são do autor humano. O uso de IA é declarado no README e na apresentação; commits
  não carregam co-autoria de IA.
- **Commits** pequenos e coesos, no padrão Conventional Commits, com build verde a cada commit.
- **Gates obrigatórios** antes de integrar: `./gradlew check` (testes unitários + cobertura +
  arquitetura), `./gradlew integrationTest`, build da imagem Docker e CodeQL.
- Todo `plan.md` DEVE conter o Constitution Check; qualquer violação DEVE ser justificada em
  "Complexity Tracking" ou o plano é rejeitado.

## Governance

- Esta constitution prevalece sobre qualquer outra prática, template ou preferência de
  ferramenta, inclusive sobre sugestões geradas por IA.
- Emendas são feitas por commit dedicado que altera este arquivo, com justificativa e
  incremento de versão semântico: MAJOR para remoção ou redefinição incompatível de princípio;
  MINOR para princípio ou seção nova ou expansão material; PATCH para esclarecimentos.
- A conformidade é verificada no Constitution Check de cada `plan.md`, na execução de
  `/speckit-analyze` antes da implementação e na revisão de cada entrega.
- Orientações operacionais de desenvolvimento ficam no `README.md` e em `docs/adr/`.

**Version**: 1.0.0 | **Ratified**: 2026-09-29 | **Last Amended**: 2026-09-29
