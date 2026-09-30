# syntax=docker/dockerfile:1

# Tags fixas (build reprodutivel): exigem rotina de atualizacao de patch (docs/adr/0015-empacotamento-e-operacao.md).
FROM eclipse-temurin:21.0.12_8-jdk-noble AS base
WORKDIR /workspace
COPY gradlew build.gradle.kts settings.gradle.kts ./
COPY gradle gradle
RUN chmod +x gradlew
COPY src src

FROM base AS test
RUN --mount=type=cache,target=/root/.gradle ./gradlew check --no-daemon

FROM base AS builder
RUN --mount=type=cache,target=/root/.gradle ./gradlew bootJar --no-daemon \
    && cp $(ls build/libs/*.jar | grep -v plain) /workspace/app.jar

FROM eclipse-temurin:21.0.12_8-jre-noble AS runtime
# Usuario nao-root (uid/gid 10001); a heap acompanha o limite de memoria do container e um OutOfMemoryError derruba o processo
# (o orquestrador reinicia) em vez de deixar a JVM meio viva.
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin app
WORKDIR /app
COPY --from=builder --chown=10001:10001 /workspace/app.jar app.jar
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
USER 10001:10001
# 8080 = API; 8082 = gerenciamento (Actuator), que NAO deve ser roteada pelo balanceador publico.
EXPOSE 8080 8082
# Liveness: so o estado do proprio processo (nunca o DynamoDB); a raiz /actuator/health nao serve de sonda.
HEALTHCHECK --interval=15s --timeout=3s --start-period=45s --retries=3 \
    CMD curl -fsS http://localhost:8082/actuator/health/liveness || exit 1
# Exec form: a JVM e o PID 1, recebe o SIGTERM e o encerramento gracioso funciona.
ENTRYPOINT ["java", "-jar", "app.jar"]
