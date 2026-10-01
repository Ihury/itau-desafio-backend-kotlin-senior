package br.com.itau.challenge.balance.adapter.input.kafka

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.assertEquals

// Sem @SpringBootTest: um contexto completo extra religa o compartilhado e inicia listeners sem broker.
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
