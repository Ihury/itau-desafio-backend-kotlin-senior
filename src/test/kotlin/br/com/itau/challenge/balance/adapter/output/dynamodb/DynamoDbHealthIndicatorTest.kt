package br.com.itau.challenge.balance.adapter.output.dynamodb

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.slf4j.LoggerFactory
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
import kotlin.test.assertTrue

class DynamoDbHealthIndicatorTest {
    /** Relogio controlavel: o cache de 5 s e provado sem dormir. */
    private class MutableClock(
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
    private val clock = MutableClock(Instant.parse("2026-01-01T00:00:00Z"))
    private val indicator = DynamoDbHealthIndicator(client, "AccountBalances", registry, clock)

    private lateinit var appender: ListAppender<ILoggingEvent>
    private val indicatorLogger = LoggerFactory.getLogger(DynamoDbHealthIndicator::class.java) as Logger

    @BeforeEach
    fun captureLogs() {
        appender = ListAppender<ILoggingEvent>().apply { start() }
        indicatorLogger.addAppender(appender)
        indicatorLogger.level = Level.DEBUG
    }

    @AfterEach
    fun releaseLogs() {
        indicatorLogger.detachAppender(appender)
    }

    private fun tableIs(status: TableStatus) {
        val response = DescribeTableResponse.builder().table(TableDescription.builder().tableName("AccountBalances").tableStatus(status).build()).build()
        doReturn(response).`when`(client).describeTable(any(DescribeTableRequest::class.java))
    }

    private fun probeFails(failure: Throwable = SdkClientException.builder().message("Unable to execute HTTP request: connect to dynamodb:8000 failed").build()) {
        doThrow(failure).`when`(client).describeTable(any(DescribeTableRequest::class.java))
    }

    private fun gauge() = registry.get("balance.dependency.up").tag("dependency", "dynamodb").gauge()

    @Test
    fun `an active table is up and the probe is a describe table of the configured table with a short timeout`() {
        tableIs(TableStatus.ACTIVE)

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
        tableIs(TableStatus.UPDATING)

        assertEquals(Status.UP, indicator.health().status)
    }

    @Test
    fun `a table that is not usable is down`() {
        listOf(TableStatus.CREATING, TableStatus.DELETING, TableStatus.INACCESSIBLE_ENCRYPTION_CREDENTIALS, TableStatus.ARCHIVING, TableStatus.ARCHIVED).forEach { status ->
            val fresh = DynamoDbHealthIndicator(client, "AccountBalances", registry, clock)
            tableIs(status)

            assertEquals(Status.DOWN, fresh.health().status, "status $status")
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
            val fresh = DynamoDbHealthIndicator(client, "AccountBalances", registry, clock)
            probeFails(failure)

            assertEquals(Status.DOWN, fresh.health().status, failure.javaClass.simpleName)
        }
    }

    @Test
    fun `a missing description of the table is down`() {
        doReturn(DescribeTableResponse.builder().build()).`when`(client).describeTable(any(DescribeTableRequest::class.java))

        assertEquals(Status.DOWN, indicator.health().status)
    }

    @Test
    fun `the result is cached for five seconds, whether up or down`() {
        tableIs(TableStatus.ACTIVE)
        repeat(3) { indicator.health() }
        clock.advance(Duration.ofMillis(4_999))
        indicator.health()

        verify(client, times(1)).describeTable(any(DescribeTableRequest::class.java))

        clock.advance(Duration.ofMillis(1))
        probeFails()
        assertEquals(Status.DOWN, indicator.health().status, "passados 5 s ha um novo probe")
        repeat(3) { indicator.health() }
        verify(client, times(2)).describeTable(any(DescribeTableRequest::class.java))

        tableIs(TableStatus.ACTIVE)
        assertEquals(Status.DOWN, indicator.health().status, "a falha tambem fica em cache")
        clock.advance(Duration.ofSeconds(5))
        assertEquals(Status.UP, indicator.health().status)
        verify(client, times(3)).describeTable(any(DescribeTableRequest::class.java))
    }

    @Test
    fun `the output never carries details, exception messages or infrastructure names`() {
        probeFails()
        val down = indicator.health()
        clock.advance(Duration.ofSeconds(5))
        tableIs(TableStatus.ACTIVE)
        val up = indicator.health()

        assertTrue(down.details.isEmpty(), "detalhes em DOWN: ${down.details}")
        assertTrue(up.details.isEmpty(), "detalhes em UP: ${up.details}")
        assertTrue(!down.toString().contains("dynamodb:8000") && !down.toString().contains("AccountBalances"))
    }

    @Test
    fun `the dependency gauge is 1 when the last probe was ok and 0 when it failed and tracks the state`() {
        tableIs(TableStatus.ACTIVE)
        assertEquals(1.0, gauge().value())

        clock.advance(Duration.ofSeconds(5))
        probeFails()
        assertEquals(0.0, gauge().value())

        clock.advance(Duration.ofSeconds(5))
        tableIs(TableStatus.ACTIVE)
        assertEquals(1.0, gauge().value())
    }

    @Test
    fun `the gauge and the health share the same cached state`() {
        tableIs(TableStatus.ACTIVE)
        assertEquals(Status.UP, indicator.health().status)
        probeFails() // dentro da janela: nem o gauge nem o health enxergam a nova falha

        assertEquals(1.0, gauge().value())
        assertEquals(Status.UP, indicator.health().status)
        verify(client, times(1)).describeTable(any(DescribeTableRequest::class.java))
    }

    @Test
    fun `the gauge has a description and the dependency tag only`() {
        tableIs(TableStatus.ACTIVE)

        assertEquals(listOf("dependency"), gauge().id.tags.map { it.key })
        assertTrue(!gauge().id.description.isNullOrBlank())
    }

    @Test
    fun `state transitions are logged once with the exception class and never the message`() {
        probeFails()
        indicator.health()
        indicator.health() // em cache: nao repete o log
        clock.advance(Duration.ofSeconds(5))
        probeFails()
        indicator.health() // continua DOWN: sem novo log de transicao
        clock.advance(Duration.ofSeconds(5))
        tableIs(TableStatus.ACTIVE)
        indicator.health()

        val transitions = appender.list.filter { it.level.isGreaterOrEqual(Level.INFO) }
        val messages = transitions.map { it.formattedMessage + it.throwableProxy?.message.orEmpty() }
        assertEquals(2, messages.size, "so as transicoes UP->DOWN e DOWN->UP: $messages")
        assertTrue(messages[0].contains("SdkClientException"), messages[0])
        assertEquals(Level.WARN, transitions[0].level)
        val everything = appender.list.map { it.formattedMessage + it.throwableProxy?.message.orEmpty() }
        assertTrue(everything.none { it.contains("dynamodb:8000") }, "a mensagem da excecao nao pode ir ao log: $everything")
        assertTrue(appender.list.any { it.level == Level.DEBUG && it.formattedMessage.contains("SdkClientException") }, "cada falha do probe deixa um log de depuracao")
    }
}
