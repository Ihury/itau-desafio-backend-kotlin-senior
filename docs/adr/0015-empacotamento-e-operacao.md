# ADR-0015: Empacotamento e operação: imagem não-root, heap relativa, healthcheck e tags fixas

- **Status**: Aceita
- **Data**: 2026-09-29
- **Referências**: Constitution v1.0.1, Princípio VII; `specs/001-consulta-saldo/research.md` R-15; `contracts/configuration.md`; FR-033 e FR-034; ADR-0013

## Contexto

O serviço roda em contêiner e precisa, em produção, (a) não executar como root, (b) dimensionar a heap pela memória do contêiner e
morrer de forma limpa quando ela acaba, (c) declarar a própria saúde ao orquestrador sem depender do DynamoDB e (d) receber o
`SIGTERM` de verdade para que o encerramento gracioso (ADR-0007, ADR-0013) preserve o que já foi persistido. O Dockerfile herdado do
starter usava tags flutuantes (`eclipse-temurin:21-jdk`/`21-jre`), rodava como root, não tinha `HEALTHCHECK` nem limite relativo de heap.

## Decisão

- **Usuário não-root** `app` com uid/gid **10001** (`groupadd`/`useradd` de sistema, sem shell nem diretório pessoal); o `app.jar` é
  copiado com `--chown` e `USER 10001:10001` vale para o `ENTRYPOINT`. O uid numérico permite `runAsNonRoot` em Kubernetes.
- **Heap relativa**: `-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError`, passadas **no `ENTRYPOINT`** e não em `JAVA_TOOL_OPTIONS`
  (que a imagem inicial usava): com `JAVA_TOOL_OPTIONS` a JVM imprime `Picked up JAVA_TOOL_OPTIONS: ...` na saída de erro, uma linha que
  não é JSON e quebra o coletor de logs estruturados. O workflow `docker.yml` confere que a variável não está no `Config.Env` da
  imagem. Os 25% restantes cobrem metaspace,
  pilhas de threads, buffers diretos (Kafka, Netty/Apache HTTP) e o próprio SO. `ExitOnOutOfMemoryError` derruba o processo em vez de
  deixar uma JVM meio viva: o orquestrador reinicia e a ingestão retoma do último offset confirmado (at-least-once).
- **`HEALTHCHECK`** com `curl -fsS http://localhost:8082/actuator/health/liveness` (a imagem `jre-noble` traz `curl`; verificado).
  É **liveness** de propósito: só o estado do processo, nunca o DynamoDB (ADR-0013). A raiz `/actuator/health` não é usada. O
  `docker-compose.yml` usa a **readiness** na mesma porta para ordenar dependências locais.
- **`ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "app.jar"]` em exec form**: a JVM é o PID 1, recebe o `SIGTERM` e o graceful shutdown
  (`server.shutdown=graceful`, 30 s por fase) funciona; a forma shell colocaria um `sh` na frente e o sinal nunca chegaria à JVM. O
  compose usa `stop_grace_period: 40s`, acima dos 30 s da fase de encerramento.
- **`EXPOSE 8080 8082`**: API e gerenciamento. A 8082 (Actuator) **não deve ser roteada pelo balanceador público**.
- **Tags fixas** `eclipse-temurin:21.0.12_8-jdk-noble` (base, builder e test) e `eclipse-temurin:21.0.12_8-jre-noble` (runtime), com
  base Ubuntu noble explícita. As tags existem no Docker Hub (puxadas na implementação). O build fica reprodutível, mas a tag fixa
  **exige rotina de atualização**: revisar o patch do Temurin a cada versão de segurança (por exemplo, com Dependabot/Renovate para
  imagens Docker) e reconstruir. Sem isso a imagem congela vulnerabilidades do SO e da JRE.
- **Imagem renomeada** de `itau-hello-world` para `consulta-saldo` (Makefile e workflow `docker.yml`), e o projeto Gradle
  (`rootProject.name` e `description`), que ainda levava o nome do starter, também passou a `consulta-saldo`.
- **`.dockerignore`** não envia `specs`, `docs`, `.specify`, `.claude`, `.github`, `infra` e `perf` ao contexto: o estágio `test` só
  precisa de `src`, do Gradle e do wrapper, então documentação não invalida o cache de build.
- Os estágios `test` (`./gradlew check`, com gate de cobertura) e `builder` continuam no mesmo Dockerfile; `make test` roda o gate
  dentro de um contêiner, igual ao CI.

## Alternativas consideradas

| Alternativa | Por que foi descartada |
|-------------|------------------------|
| Tags flutuantes (`21-jre`) | Recebem patches sozinhas, mas o build deixa de ser reprodutível e uma mudança de base pode quebrar a imagem sem alteração no repositório |
| Digest (`@sha256:...`) em vez de tag | Ainda mais reprodutível, porém ilegível na revisão; a tag com patch e base explícitos é o equilíbrio para um repositório de avaliação |
| `JAVA_OPTS` com `-Xmx` fixo | Não acompanha o limite de memória do contêiner; `MaxRAMPercentage` se adapta ao que o orquestrador define |
| Distroless / imagem `jlink` mínima | Sem `curl` para o `HEALTHCHECK` e mais complexidade de build; ganho de superfície não compensa nesta versão |
| `HEALTHCHECK` na readiness | Acoplaria o reinício do contêiner a um estado transitório; liveness é a pergunta certa para "reiniciar?" |
| Buildpacks / Jib | Trocam o Dockerfile já padronizado (estágios `test`/`builder`) por outra cadeia sem ganho para este escopo |

## Consequências

- (+) Imagem sem root, com heap proporcional, saúde declarada e encerramento gracioso comprovável (`docker compose stop app` conclui
  em menos de 40 s com log de encerramento).
- (+) Build reprodutível: o mesmo commit gera a mesma base.
- (-) Tags fixas exigem manutenção periódica; sem a rotina, a base envelhece.
- (-) O `timeout-per-shutdown-phase` de 30 s vale **por fase** do ciclo de vida (o servidor web e o container Kafka são fases
  distintas): o tempo total de encerramento pode passar de 30 s. Em orquestrador, `terminationGracePeriodSeconds` >= 60 s e, atrás de
  balanceador, `preStop` e *deregistration delay* (ver "Rodando na AWS" no README).
- (-) `MaxRAMPercentage=75` é uma heurística: cargas com muitos buffers diretos podem pedir um percentual menor ou mais memória.
- (-) Sem `USER` root não há como instalar pacotes em runtime; qualquer ferramenta extra precisa entrar no build da imagem.
