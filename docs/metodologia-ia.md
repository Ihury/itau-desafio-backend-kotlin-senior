# Metodologia de desenvolvimento com IA

Este documento explica **como a IA foi usada** neste projeto, quem decidiu o quê e como cada saída foi verificada. O uso de IA foi
**autorizado pelo Itaú** sob a condição de que a metodologia fosse explicada na apresentação; este texto é essa explicação e
também um roteiro de auditoria: tudo o que é afirmado aqui pode ser conferido no repositório (ver [Como reproduzir e auditar](#como-reproduzir-e-auditar)).

## Resumo em cinco linhas

1. O trabalho seguiu **Spec-Driven Development** com o GitHub Spec Kit v1.0.13 e o Claude Code: primeiro a especificação e as
   decisões, depois as tarefas, só então o código.
2. Um agente **orquestrador** (Claude Opus 5.5) conduziu o processo e revisou tudo; **subagentes executores** (Claude Sonnet 5.5)
   executaram cada etapa do Spec Kit.
3. As **decisões de negócio e de arquitetura foram do autor humano**, tomadas em perguntas de esclarecimento e nas revisões.
4. Toda saída de IA passou por **gates automáticos** (testes escritos antes do código, cobertura >= 90%, teste de arquitetura) e
   pela **revisão do orquestrador** a cada fase, com correções reais registradas abaixo.
5. Os artefatos ficam versionados (`specs/`, `.specify/`, `docs/adr/`), os commits são pequenos e **não têm co-autoria de IA**.

## Ferramentas e papéis

| Papel | Quem | O que fez |
|-|-|-|
| Autor e responsável | Pessoa humana | Aprovou a constitution, respondeu às perguntas do `clarify`, decidiu nas revisões do plano, autorizou a estratégia de implementação e é dona do resultado |
| Orquestrador | Claude Opus 5.5 (Claude Code) | Redigiu a constitution a partir da análise do desafio, escreveu os briefings de cada etapa, revisou cada artefato contra a constitution e o enunciado, levou as decisões ao autor e validou cada fase reexecutando os gates |
| Executores | Subagentes Claude Sonnet 5.5 | Executaram cada etapa do Spec Kit: `specify`, `clarify`, `plan`, `tasks`, `analyze` e `implement`, este último fase a fase |
| Método | GitHub Spec Kit v1.0.13 | Fluxo `constitution -> specify -> clarify -> plan -> tasks -> analyze -> implement`, com artefatos em arquivos versionados |

O enunciado original **não está no repositório** e nunca foi copiado para dentro dele; o README e a spec descrevem o serviço com
palavras próprias.

## Fluxo e o que cada etapa produziu

| Etapa | Produto versionado | Observações |
|-|-|-|
| **Constitution** | `.specify/memory/constitution.md` | v1.0.0 redigida pelo orquestrador a partir da análise do desafio e aprovada pelo autor. Uma emenda patch (v1.0.1) esclareceu duas frases sem mudar a intenção dos princípios: normalização de zeros à direita não é perda de valor, e a saúde das dependências não tira a instância de rotação |
| **Specify** | `specs/001-consulta-saldo/spec.md`, `checklists/requirements.md` | Histórias de usuário priorizadas, requisitos (FR) e critérios de sucesso (SC) |
| **Clarify** | seção *Clarifications* da `spec.md` | Duas rodadas registradas: a primeira com cinco perguntas ao autor (DECLINED, DISABLED, timestamp futuro, resposta para conta desabilitada, status desconhecido) e a de revisão do plano com três (readiness, limite inferior de timestamps, representação do saldo) |
| **Plan** | `plan.md`, `research.md`, `data-model.md`, `contracts/*`, `quickstart.md` | Decisões com alternativas; **spikes descartáveis contra DynamoDB Local e Redpanda reais** validaram hipóteses (ordenação de UUID, DLT com número de partições diferente, comportamento do consumer com o DLT ausente, entre outras) antes de virarem decisão |
| **Tasks** | `tasks.md` | 181 tarefas em 39 unidades de commit, com testes antes da implementação e a regra "nunca commitar vermelho" |
| **Analyze** | remediação aplicada em commit próprio (`docs: aplica remediacao do analyze na feature 001`) | 27 achados (0 critical, 2 high, 11 medium, 14 low); todos remediados antes de escrever código |
| **Implement** | código, testes, ADRs, `tasks.md` com as notas de execução | Uma fase por vez; entre as fases o orquestrador revisou e reexecutou os gates |

## Decisões tomadas pelo autor humano

Estas decisões **não foram da IA**: vieram do `clarify` e das revisões, e a IA as implementou.

- Transações `DECLINED` **participam da precedência** e atualizam o snapshot.
- Conta `DISABLED` **atualiza o snapshot**, mas a consulta responde **409 `conta-desabilitada`**, sem saldo nem titular.
- **Tolerância configurável** para timestamp futuro; o relógio do serviço só valida, nunca decide precedência.
- Status de conta desconhecido é **rejeitado e isolado** (DLT), preservando o conteúdo.
- **Readiness independente do DynamoDB**: a saúde das dependências fica em grupo próprio e em métrica.
- Saldo como **`N` no DynamoDB e `BigDecimal`** ponta a ponta, com a escala completada às casas da moeda, decidido após análise
  crítica de alternativas (`S` texto e `N` em centavos).
- Aprovação das decisões do plano e da **estratégia de implementação por fase**.
- **Commits em português, sem acentos e sem co-autoria de IA.**

## Correções que a revisão introduziu sobre o que a IA produziu

A revisão humana e do orquestrador não foi formalidade: encontrou defeitos reais nas saídas dos executores, que foram corrigidos
antes de seguir. Exemplos:

| O que a IA produziu | Problema encontrado na revisão | Correção |
|-|-|-|
| Spec com contagem de desfechos | **Dupla contagem** de desfechos de uma mesma mensagem | Regra "exatamente um desfecho por mensagem" (FR-031/SC-010), com teste que reconcilia a soma das métricas com as mensagens publicadas |
| Limite inferior de 2000-01-01 para timestamps | Aplicado também a `account.created_at`: **rejeitaria contas legítimas mais antigas** | Limites distintos: 2000-01-01 para `transaction.timestamp` e 1900-01-01 para `account.created_at` (aceita instantes negativos em µs) |
| Readiness com verificação do DynamoDB | Dependência compartilhada na readiness **tiraria todas as instâncias de rotação** ao mesmo tempo e anularia o 503 rápido do circuit breaker | Readiness só do processo; dependências em `/actuator/health/dependencies` e no gauge `balance.dependency.up` |
| Saldo como `S` (texto) | Preserva uma escala que o contrato não garante e trata dinheiro como texto, não como número no banco | `N` + `BigDecimal` (as alternativas `S` e `N` em centavos foram analisadas criticamente e registradas no ADR-0005), com normalização de zeros tratada como não-perda e escala completada só na resposta |
| Error handler padrão do Spring Kafka | **Janela em que mensagens seriam descartadas antes da DLT** (achado do `analyze`) | O handler sem descarte nasce junto com o consumer (C18) e as unidades seguintes só o estendem; nenhum commit intermediário permite perder mensagem |
| Contradição da escrita condicional classificada como falha transitória | Reentregaria para sempre e **bloquearia a partição** se a causa fosse permanente | Reclassificada como "não classificada": 3 entregas e DLT `unprocessable_event` |
| Item corrompido na leitura | Poderia virar 400 ou 404 | Tratado como **500** (defeito nosso), com métrica `balance.store.read.corrupted` |
| Saída de uma dependência de teste (jqwik) | Continha texto dirigido a agentes de IA pedindo que os resultados fossem ignorados | O texto foi **ignorado** (não é instrução do usuário) e a dependência **descartada**, além de outras razões técnicas em `research.md` R-14 |

## Salvaguardas

- **TDD com evidência de vermelho**: em cada unidade o teste é escrito e falha antes da implementação. Para os testes de
  integração escritos depois do código, a evidência é uma **mutação temporária descartável** (por exemplo, inverter a condição da
  escrita ou usar "o último a chegar vence") que faz o teste falhar, registrada na nota de execução da tarefa correspondente.
- **Gates em todo commit**: `./gradlew check` com **JaCoCo >= 90%** (nunca reduzido, sem exclusões) e o teste de arquitetura
  **Konsist**; `make integration-test` (DynamoDB Local e Redpanda reais) sempre que a unidade toca infraestrutura ou testes de
  integração.
- **Revisão a cada fase**: o orquestrador reexecutou os gates, leu os diffs contra a constitution e devolveu ajustes.
- **Mensagens de commit verificadas**: Conventional Commits em português, só ASCII (checado por `LC_ALL=C grep '[^ -~]'`).
- **Nenhum `push` sem autorização** do autor; o histórico de trabalho fica local até ele decidir publicar.
- **Enunciado fora do repositório** e README que não o reproduz.
- **Trilha de auditoria versionada**: `specs/`, `.specify/` e `docs/adr/` (15 ADRs com contexto, decisão, alternativas e
  consequências) mostram o que foi decidido e por quê; o `tasks.md` guarda, tarefa a tarefa, as notas de execução (fatos reais,
  divergências e evidências).
- **Privacidade por construção**: teste com valores sentinela garante que logs não contêm saldo, titular nem payload.
- **Sem co-autoria de IA nos commits**. O uso de IA é declarado **neste documento e no README**, não nos commits.

## Como reproduzir e auditar

| Quero verificar | Onde olhar |
|-|-|
| O que foi decidido e por quê | `specs/001-consulta-saldo/` (`spec.md` com as *Clarifications*, `plan.md`, `research.md`, `data-model.md`, `contracts/`, `quickstart.md`) e `.specify/memory/constitution.md` |
| Cada decisão de arquitetura | `docs/adr/0001` a `0015` |
| O que cada tarefa fez, com evidências | `specs/001-consulta-saldo/tasks.md` (marcações `[X]` e *Notas de execução*) |
| A ordem do trabalho | `git log --oneline`: constitution, spec, clarificações, emenda, plano, tarefas, remediação do analyze e depois um commit por unidade (fases 1 a 9), sempre com o teste junto da implementação |
| Que não há co-autoria de IA | `git log --format=%B \| grep -i 'co-authored-by'` (sem resultado). O único commit cuja mensagem cita a ferramenta é o que inicializa o Spec Kit e descreve a integração `.claude/skills` |
| Que as mensagens são ASCII | `git log --format=%B \| LC_ALL=C grep -n '[^ -~]'` (sem resultado) |
| Que o código funciona | `./gradlew check` (testes unitários, Konsist, JaCoCo), `make test` (o mesmo em container) e `make integration-test` (infraestrutura real, incluindo caos com `docker compose pause`) |
| O comportamento ponta a ponta | `make up`, `make kafka-produce-scenario` e o roteiro de `specs/001-consulta-saldo/quickstart.md` |
| Que cada commit compila e passa | `git rebase --exec "./gradlew check" <commit-base>` em uma cópia descartável do repositório |

## Limites

- **A IA não decidiu regras de negócio nem trade-offs de arquitetura por conta própria.** Ela propôs opções e alternativas; quem
  escolheu foi o autor (lista acima). Onde havia dúvida, a pergunta foi levada ao autor e a resposta ficou registrada na spec.
- **A responsabilidade pelo resultado é do autor.** Ele leu, aprovou e responde por spec, decisões, código e documentação; a IA
  é ferramenta, não autora.
- **A IA erra**, e os exemplos acima mostram isso. Por isso nada foi aceito sem gate automático e revisão; ainda assim, testes só
  provam o que foram escritos para provar.
- **O que não foi verificado consta como tal**: por exemplo, a meta de latência sob carga (SC-001) depende de um teste de carga
  opcional que não foi executado, e o DynamoDB Local não reproduz throttling nem latência reais da AWS. Ver a seção de riscos do
  README.
- **A confirmação dos workflows do GitHub Actions** depende do push autorizado pelo autor; até lá valem os equivalentes locais
  registrados no `tasks.md`.
- A qualidade do resultado depende de quem revisa: este processo reduz o risco, mas não o elimina.
