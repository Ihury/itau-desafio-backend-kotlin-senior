package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import br.com.itau.challenge.balance.testing.InMemoryBalanceStore
import br.com.itau.challenge.balance.testing.RecordingProcessingMetrics
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * Timestamp no futuro alem da tolerancia e invalido (FR-012). O relogio SO valida: nunca participa da precedencia (FR-003),
 * e a rejeicao acontece antes de qualquer escrita.
 */
class FutureTimestampToleranceTest {
    private val now = Instant.parse("2026-06-01T12:00:00Z")
    private val store = InMemoryBalanceStore()
    private val metrics = RecordingProcessingMetrics()

    private fun service(
        at: Instant = now,
        tolerance: Duration = Duration.ofMinutes(5),
    ) = ProcessTransactionEventService(store, metrics, Clock.fixed(at, ZoneOffset.UTC), FutureTolerance(tolerance))

    private fun micros(instant: Instant): Long = instant.epochSecond * 1_000_000L + instant.nano / 1_000L

    private val account = AccountId.parse("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")

    @Test
    fun `a transaction timestamp exactly at now plus the tolerance is accepted`() {
        val result = service().process(transactionEvent(timestampMicros = micros(now.plus(Duration.ofMinutes(5)))))

        assertEquals(ApplyResult.Applied, result)
        assertNotNull(store.current(account))
    }

    @Test
    fun `one microsecond beyond the tolerance is rejected and the writer is never called`() {
        val beyond = micros(now.plus(Duration.ofMinutes(5))) + 1

        val failure = assertFailsWith<InvalidEventException> { service().process(transactionEvent(timestampMicros = beyond)) }

        assertEquals(RejectionReason.INVALID_TIMESTAMP, failure.reason)
        assertEquals("transaction.timestamp", failure.detail)
        assertNull(store.current(account), "o armazenamento nao pode ser tocado")
        assertEquals(emptyList(), metrics.outcomes)
    }

    @Test
    fun `an account creation beyond the tolerance is rejected with its own field path`() {
        val beyond = micros(now.plus(Duration.ofMinutes(5))) + 1

        val failure =
            assertFailsWith<InvalidEventException> {
                service().process(transactionEvent(timestampMicros = micros(now), accountCreatedAtMicros = beyond))
            }

        assertEquals(RejectionReason.INVALID_TIMESTAMP, failure.reason)
        assertEquals("account.created_at", failure.detail)
        assertNull(store.current(account))
    }

    @Test
    fun `an account creation at the limit and an old one from 1998 are valid`() {
        val service = service()

        assertEquals(
            ApplyResult.Applied,
            service.process(transactionEvent(timestampMicros = micros(now), accountCreatedAtMicros = micros(now.plus(Duration.ofMinutes(5))))),
        )
        assertEquals(
            ApplyResult.Applied,
            service.process(
                transactionEvent(
                    transactionId = "11111111-b154-48b5-9f3e-553935cc4543",
                    timestampMicros = micros(now) + 1,
                    accountCreatedAtMicros = 899_251_200_000_000L,
                ),
            ),
        )
    }

    @Test
    fun `the transaction timestamp is checked before the account creation`() {
        val beyond = micros(now.plus(Duration.ofHours(1)))

        val failure =
            assertFailsWith<InvalidEventException> {
                service().process(transactionEvent(timestampMicros = beyond, accountCreatedAtMicros = beyond))
            }

        assertEquals("transaction.timestamp", failure.detail)
    }

    @Test
    fun `an event in the future but inside the tolerance is processed normally and keeps its own instant`() {
        val inside = micros(now.plus(Duration.ofMinutes(2)))

        assertEquals(ApplyResult.Applied, service().process(transactionEvent(timestampMicros = inside)))

        assertEquals(inside, store.current(account)?.precedence?.timestamp?.micros)
    }

    @Test
    fun `the tolerance is configurable`() {
        val threeMinutes = micros(now.plus(Duration.ofMinutes(3)))

        assertEquals(ApplyResult.Applied, service(tolerance = Duration.ofMinutes(5)).process(transactionEvent(timestampMicros = threeMinutes)))
        val failure =
            assertFailsWith<InvalidEventException> {
                service(tolerance = Duration.ofMinutes(1)).process(
                    transactionEvent(transactionId = "22222222-b154-48b5-9f3e-553935cc4543", timestampMicros = threeMinutes),
                )
            }
        assertEquals("transaction.timestamp", failure.detail)
    }

    @Test
    fun `the clock only validates and never changes which event wins`() {
        val older = transactionEvent(transactionId = "33333333-b154-48b5-9f3e-553935cc4543", timestampMicros = micros(now), balanceAmount = "1.00")
        val newer = transactionEvent(transactionId = "44444444-b154-48b5-9f3e-553935cc4543", timestampMicros = micros(now) + 1, balanceAmount = "2.00")

        val results =
            listOf(now, now.plus(Duration.ofDays(30)), now.minus(Duration.ofMinutes(1))).map { at ->
                val fresh = InMemoryBalanceStore()
                val service = ProcessTransactionEventService(fresh, RecordingProcessingMetrics(), Clock.fixed(at, ZoneOffset.UTC), FutureTolerance(Duration.ofMinutes(5)))
                service.process(newer)
                service.process(older)
                fresh.current(account)?.balance?.amount
            }

        assertEquals(setOf(BigDecimal("2.00")), results.toSet())
    }
}
