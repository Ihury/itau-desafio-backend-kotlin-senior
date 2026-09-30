package br.com.itau.challenge.balance.adapter.input.kafka

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.assertEquals

/**
 * Os parametros do backoff da falha transiente vem das variaveis de ambiente
 * (`KAFKA_BACKOFF_INITIAL_MS`, `KAFKA_BACKOFF_MAX_MS`, `KAFKA_BACKOFF_JITTER_MS`), nao de constantes no codigo. Roda so a
 * configuracao de backpressure com o `application.yaml` real, sem contexto completo (um segundo contexto completo faria o Spring
 * pausar e religar o contexto compartilhado, iniciando os listeners Kafka nos testes sem broker).
 */
class BackpressureEnvOverrideTest {
    private val runner =
        ApplicationContextRunner()
            .withInitializer(ConfigDataApplicationContextInitializer())
            .withUserConfiguration(BackpressureConfig::class.java)

    @Test
    fun `the defaults of the yaml are 500 ms, thirty seconds and 250 ms of jitter`() {
        runner.run { context ->
            assertEquals(BackOffProperties(initialMs = 500, maxMs = 30_000, jitterMs = 250), context.getBean(BackOffProperties::class.java))
        }
    }

    @Test
    fun `the back off parameters follow the environment variables`() {
        runner
            .withPropertyValues("KAFKA_BACKOFF_INITIAL_MS=100", "KAFKA_BACKOFF_MAX_MS=8000", "KAFKA_BACKOFF_JITTER_MS=40")
            .run { context ->
                assertEquals(BackOffProperties(initialMs = 100, maxMs = 8_000, jitterMs = 40), context.getBean(BackOffProperties::class.java))
            }
    }
}
