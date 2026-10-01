package br.com.itau.challenge.balance.config

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.assertEquals

class ManagementPropertiesTest {
    private val runner = ApplicationContextRunner().withInitializer(ConfigDataApplicationContextInitializer())

    @Test
    fun `the management port defaults to 8082 and the api stays on 8080`() {
        runner.run { context ->
            assertEquals("8082", context.environment.getProperty("management.server.port"))
            assertEquals("8080", context.environment.getProperty("server.port"))
        }
    }

    @Test
    fun `the management port follows the environment variable`() {
        runner.withPropertyValues("MANAGEMENT_SERVER_PORT=9099").run { context ->
            assertEquals("9099", context.environment.getProperty("management.server.port"))
        }
    }

    @Test
    fun `only health, info and prometheus are exposed and the listener publishes observations`() {
        runner.run { context ->
            assertEquals("health,info,prometheus", context.environment.getProperty("management.endpoints.web.exposure.include"))
            assertEquals("true", context.environment.getProperty("spring.kafka.listener.observation-enabled"))
        }
    }
}
