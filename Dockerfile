# syntax=docker/dockerfile:1

FROM eclipse-temurin:21-jdk AS base
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

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
COPY --from=builder /workspace/app.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
