package br.com.itau.challenge.balance.config

import br.com.itau.challenge.balance.adapter.output.dynamodb.CircuitBreakerProperties
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

@SpringBootTest
@ActiveProfiles("test")
class ResilienceConfigTest {
    @Autowired
    private lateinit var properties: CircuitBreakerProperties

    @Autowired
    private lateinit var registry: CircuitBreakerRegistry

    @Autowired
    private lateinit var readBreaker: CircuitBreaker

    @Autowired
    private lateinit var meterRegistry: MeterRegistry

    @Test
    fun `properties map the documented defaults`() {
        assertEquals(Duration.ofSeconds(10), properties.window)
        assertEquals(20, properties.minCalls)
        assertEquals(50f, properties.failureRate)
        assertEquals(Duration.ofMillis(500), properties.slowCall)
        assertEquals(80f, properties.slowRate)
        assertEquals(Duration.ofSeconds(10), properties.openWait)
        assertEquals(5, properties.halfOpenCalls)
    }

    @Test
    fun `registry provides the dynamodb read breaker with the configured thresholds`() {
        assertEquals("dynamodb-read", readBreaker.name)
        assertSame(readBreaker, registry.circuitBreaker("dynamodb-read"))
        val config = readBreaker.circuitBreakerConfig
        assertEquals(CircuitBreakerConfig.SlidingWindowType.TIME_BASED, config.slidingWindowType)
        assertEquals(10, config.slidingWindowSize)
        assertEquals(20, config.minimumNumberOfCalls)
        assertEquals(50f, config.failureRateThreshold)
        assertEquals(80f, config.slowCallRateThreshold)
        assertEquals(Duration.ofMillis(500), config.slowCallDurationThreshold)
        assertEquals(5, config.permittedNumberOfCallsInHalfOpenState)
        assertTrue(config.isAutomaticTransitionFromOpenToHalfOpenEnabled)
        assertFalse(config.isWritableStackTraceEnabled, "a rejeicao com o circuito aberto nao preenche pilha (custo e ruido por requisicao)")
    }

    @Test
    fun `the breaker state gauge is registered with closed as 1 in a micrometer registry`() {
        val simpleRegistry = SimpleMeterRegistry()
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(simpleRegistry)

        val closed = simpleRegistry.find("resilience4j.circuitbreaker.state").tag("name", "dynamodb-read").tag("state", "closed").gauge()
        assertNotNull(closed)
        assertEquals(1.0, closed.value())
    }

    @Test
    fun `the application meter registry already carries the breaker metrics`() {
        assertNotNull(meterRegistry.find("resilience4j.circuitbreaker.state").tag("name", "dynamodb-read").gauge())
    }
}
