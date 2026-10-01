package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.testing.LogCapture
import ch.qos.logback.classic.Level
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.springframework.boot.health.contributor.Status
import software.amazon.awssdk.core.exception.ApiCallTimeoutException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.TableDescription
import software.amazon.awssdk.services.dynamodb.model.TableStatus
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DynamoDbHealthIndicatorTest {
    private class AdvanceableClock(
        private var now: Instant,
    ) : Clock() {
        fun advance(duration: Duration) {
            now = now.plus(duration)
        }

        override fun getZone(): ZoneId = ZoneId.of("UTC")

        override fun withZone(zone: ZoneId): Clock = this

        override fun instant(): Instant = now
    }

    private val client = mock(DynamoDbClient::class.java)
    private val registry = SimpleMeterRegistry()
    private val clock = AdvanceableClock(Instant.parse("2026-01-01T00:00:00Z"))
    private val indicator = DynamoDbHealthIndicator(client, "AccountBalances", registry, clock)

    @JvmField
    @RegisterExtension
    val logs = LogCapture(DynamoDbHealthIndicator::class.java, Level.DEBUG)

    private fun stubTableStatus(status: TableStatus) {
        val response = DescribeTableResponse.builder().table(TableDescription.builder().tableName("AccountBalances").tableStatus(status).build()).build()
        doReturn(response).`when`(client).describeTable(any(DescribeTableRequest::class.java))
    }

    private fun stubProbeFailure(failure: Throwable = SdkClientException.builder().message("Unable to execute HTTP request: connect to dynamodb:8000 failed").build()) {
        doThrow(failure).`when`(client).describeTable(any(DescribeTableRequest::class.java))
    }

    private fun gauge() = registry.get("balance.dependency.up").tag("dependency", "dynamodb").gauge()

    @Test
    fun `an active table is up and the probe is a describe table of the configured table with a short timeout`() {
        stubTableStatus(TableStatus.ACTIVE)

        val health = indicator.health()

        assertEquals(Status.UP, health.status)
        val captor = ArgumentCaptor.forClass(DescribeTableRequest::class.java)
        verify(client).describeTable(captor.capture())
        assertEquals("AccountBalances", captor.value.tableName())
        val override = captor.value.overrideConfiguration().orElseThrow()
        assertTrue(override.apiCallTimeout().orElseThrow() <= Duration.ofSeconds(1), "timeout curto da chamada")
        assertTrue(override.apiCallAttemptTimeout().orElseThrow() <= Duration.ofSeconds(1), "timeout curto da tentativa")
    }

    @Test
    fun `a table that is updating still serves reads`() {
        stubTableStatus(TableStatus.UPDATING)

        assertEquals(Status.UP, indicator.health().status)
    }

    @Test
    fun `a table that is not usable is down`() {
        listOf(TableStatus.CREATING, TableStatus.DELETING, TableStatus.INACCESSIBLE_ENCRYPTION_CREDENTIALS, TableStatus.ARCHIVING, TableStatus.ARCHIVED).forEach { status ->
            val freshIndicator = DynamoDbHealthIndicator(client, "AccountBalances", registry, clock)
            stubTableStatus(status)

            assertEquals(Status.DOWN, freshIndicator.health().status, "status $status")
        }
    }

    @Test
    fun `any failure of the probe is down`() {
        listOf(
            SdkClientException.builder().message("x").build(),
            ApiCallTimeoutException.builder().message("x").build(),
            ResourceNotFoundException.builder().message("x").build(),
            IllegalStateException("x"),
        ).forEach { failure ->
            val freshIndicator = DynamoDbHealthIndicator(client, "AccountBalances", registry, clock)
            stubProbeFailure(failure)

            assertEquals(Status.DOWN, freshIndicator.health().status, failure.javaClass.simpleName)
        }
    }

    @Test
    fun `a missing description of the table is down`() {
        doReturn(DescribeTableResponse.builder().build()).`when`(client).describeTable(any(DescribeTableRequest::class.java))

        assertEquals(Status.DOWN, indicator.health().status)
    }

    @Test
    fun `the result is cached for five seconds, whether up or down`() {
        stubTableStatus(TableStatus.ACTIVE)
        repeat(3) { indicator.health() }
        clock.advance(Duration.ofMillis(4_999))
        indicator.health()

        verify(client, times(1)).describeTable(any(DescribeTableRequest::class.java))

        clock.advance(Duration.ofMillis(1))
        stubProbeFailure()
        assertEquals(Status.DOWN, indicator.health().status, "passados 5 s ha um novo probe")
        repeat(3) { indicator.health() }
        verify(client, times(2)).describeTable(any(DescribeTableRequest::class.java))

        stubTableStatus(TableStatus.ACTIVE)
        assertEquals(Status.DOWN, indicator.health().status, "a falha tambem fica em cache")
        clock.advance(Duration.ofSeconds(5))
        assertEquals(Status.UP, indicator.health().status)
        verify(client, times(3)).describeTable(any(DescribeTableRequest::class.java))
    }

    @Test
    fun `the output never carries details, exception messages or infrastructure names`() {
        stubProbeFailure()
        val down = indicator.health()
        clock.advance(Duration.ofSeconds(5))
        stubTableStatus(TableStatus.ACTIVE)
        val up = indicator.health()

        assertTrue(down.details.isEmpty(), "detalhes em DOWN: ${down.details}")
        assertTrue(up.details.isEmpty(), "detalhes em UP: ${up.details}")
        assertFalse(down.toString().contains("dynamodb:8000") || down.toString().contains("AccountBalances"))
    }

    @Test
    fun `the dependency gauge is 1 when the last probe was ok and 0 when it failed and tracks the state`() {
        stubTableStatus(TableStatus.ACTIVE)
        assertEquals(1.0, gauge().value())

        clock.advance(Duration.ofSeconds(5))
        stubProbeFailure()
        assertEquals(0.0, gauge().value())

        clock.advance(Duration.ofSeconds(5))
        stubTableStatus(TableStatus.ACTIVE)
        assertEquals(1.0, gauge().value())
    }

    @Test
    fun `the gauge and the health share the same cached state, so neither sees a failure inside the cache window`() {
        stubTableStatus(TableStatus.ACTIVE)
        assertEquals(Status.UP, indicator.health().status)
        stubProbeFailure()

        assertEquals(1.0, gauge().value())
        assertEquals(Status.UP, indicator.health().status)
        verify(client, times(1)).describeTable(any(DescribeTableRequest::class.java))
    }

    @Test
    fun `the gauge has a description and the dependency tag only`() {
        stubTableStatus(TableStatus.ACTIVE)

        assertEquals(listOf("dependency"), gauge().id.tags.map { it.key })
        assertFalse(gauge().id.description.isNullOrBlank())
    }

    @Test
    fun `state transitions are logged once with the exception class and never the message`() {
        firstFailingProbe()
        cachedHealthCall()
        probeStillFailingAfterCacheExpiry()
        probeRecoversAfterCacheExpiry()

        val transitions = logs.events.filter { it.level.isGreaterOrEqual(Level.INFO) }
        val messages = transitions.map { it.formattedMessage + it.throwableProxy?.message.orEmpty() }
        assertEquals(2, messages.size, "so as transicoes UP->DOWN e DOWN->UP: $messages")
        assertTrue(messages[0].contains("SdkClientException"), messages[0])
        assertEquals(Level.WARN, transitions[0].level)
        val everything = logs.events.map { it.formattedMessage + it.throwableProxy?.message.orEmpty() }
        assertTrue(everything.none { it.contains("dynamodb:8000") }, "a mensagem da excecao nao pode ir ao log: $everything")
        assertTrue(logs.events.any { it.level == Level.DEBUG && it.formattedMessage.contains("SdkClientException") }, "cada falha do probe deixa um log de depuracao")
    }

    private fun firstFailingProbe() {
        stubProbeFailure()
        indicator.health()
    }

    private fun cachedHealthCall() {
        indicator.health()
    }

    private fun probeStillFailingAfterCacheExpiry() {
        clock.advance(Duration.ofSeconds(5))
        stubProbeFailure()
        indicator.health()
    }

    private fun probeRecoversAfterCacheExpiry() {
        clock.advance(Duration.ofSeconds(5))
        stubTableStatus(TableStatus.ACTIVE)
        indicator.health()
    }
}
