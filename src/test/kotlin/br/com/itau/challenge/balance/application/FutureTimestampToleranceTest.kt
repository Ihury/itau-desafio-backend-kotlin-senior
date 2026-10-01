package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.testing.InMemoryBalanceStore
import br.com.itau.challenge.balance.testing.RecordingProcessingMetrics
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import br.com.itau.challenge.balance.testing.toEpochMicros
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

class FutureTimestampToleranceTest {
    private val now = Instant.parse("2026-06-01T12:00:00Z")
    private val store = InMemoryBalanceStore()
    private val metrics = RecordingProcessingMetrics()
    private val accountId = AccountId.parse(DEFAULT_ACCOUNT_ID)

    private fun serviceAt(
        at: Instant = now,
        tolerance: Duration = Duration.ofMinutes(5),
    ) = ProcessTransactionEventService(store, metrics, Clock.fixed(at, ZoneOffset.UTC), FutureTolerance(tolerance))

    @Test
    fun `a transaction timestamp exactly at now plus the tolerance is accepted`() {
        val result = serviceAt().process(transactionEvent(timestampMicros = now.plus(Duration.ofMinutes(5)).toEpochMicros()))

        assertEquals(ApplyResult.Applied, result)
        assertNotNull(store.peek(accountId))
    }

    @Test
    fun `one microsecond beyond the tolerance is rejected and the writer is never called`() {
        val beyond = now.plus(Duration.ofMinutes(5)).toEpochMicros() + 1

        val failure = assertFailsWith<InvalidEventException> { serviceAt().process(transactionEvent(timestampMicros = beyond)) }

        assertEquals(RejectionReason.INVALID_TIMESTAMP, failure.reason)
        assertEquals("transaction.timestamp", failure.fieldPath)
        assertNull(store.peek(accountId), "o armazenamento nao pode ser tocado")
        assertEquals(emptyList(), metrics.outcomes)
    }

    @Test
    fun `an account creation beyond the tolerance is rejected with its own field path`() {
        val beyond = now.plus(Duration.ofMinutes(5)).toEpochMicros() + 1

        val failure =
            assertFailsWith<InvalidEventException> {
                serviceAt().process(transactionEvent(timestampMicros = now.toEpochMicros(), accountCreatedAtMicros = beyond))
            }

        assertEquals(RejectionReason.INVALID_TIMESTAMP, failure.reason)
        assertEquals("account.created_at", failure.fieldPath)
        assertNull(store.peek(accountId))
    }

    @Test
    fun `an account creation exactly at the limit is valid`() {
        val result =
            serviceAt().process(
                transactionEvent(timestampMicros = now.toEpochMicros(), accountCreatedAtMicros = now.plus(Duration.ofMinutes(5)).toEpochMicros()),
            )

        assertEquals(ApplyResult.Applied, result)
    }

    @Test
    fun `an account creation from 1998 is valid`() {
        val result =
            serviceAt().process(
                transactionEvent(
                    transactionId = "11111111-b154-48b5-9f3e-553935cc4543",
                    timestampMicros = now.toEpochMicros() + 1,
                    accountCreatedAtMicros = 899_251_200_000_000L,
                ),
            )

        assertEquals(ApplyResult.Applied, result)
    }

    @Test
    fun `the transaction timestamp is checked before the account creation`() {
        val beyond = now.plus(Duration.ofHours(1)).toEpochMicros()

        val failure =
            assertFailsWith<InvalidEventException> {
                serviceAt().process(transactionEvent(timestampMicros = beyond, accountCreatedAtMicros = beyond))
            }

        assertEquals("transaction.timestamp", failure.fieldPath)
    }

    @Test
    fun `an event in the future but inside the tolerance is processed normally and keeps its own instant`() {
        val inside = now.plus(Duration.ofMinutes(2)).toEpochMicros()

        assertEquals(ApplyResult.Applied, serviceAt().process(transactionEvent(timestampMicros = inside)))

        assertEquals(inside, store.peek(accountId)?.precedence?.timestamp?.micros)
    }

    @Test
    fun `a timestamp three minutes ahead is accepted when the configured tolerance is five minutes`() {
        val threeMinutes = now.plus(Duration.ofMinutes(3)).toEpochMicros()

        assertEquals(ApplyResult.Applied, serviceAt(tolerance = Duration.ofMinutes(5)).process(transactionEvent(timestampMicros = threeMinutes)))
    }

    @Test
    fun `a timestamp three minutes ahead is rejected when the configured tolerance is one minute`() {
        val threeMinutes = now.plus(Duration.ofMinutes(3)).toEpochMicros()

        val failure =
            assertFailsWith<InvalidEventException> {
                serviceAt(tolerance = Duration.ofMinutes(1)).process(
                    transactionEvent(transactionId = "22222222-b154-48b5-9f3e-553935cc4543", timestampMicros = threeMinutes),
                )
            }

        assertEquals("transaction.timestamp", failure.fieldPath)
    }

    @Test
    fun `the clock only validates and never changes which event wins`() {
        val older = transactionEvent(transactionId = "33333333-b154-48b5-9f3e-553935cc4543", timestampMicros = now.toEpochMicros(), balanceAmount = "1.00")
        val newer = transactionEvent(transactionId = "44444444-b154-48b5-9f3e-553935cc4543", timestampMicros = now.toEpochMicros() + 1, balanceAmount = "2.00")

        val results =
            listOf(now, now.plus(Duration.ofDays(30)), now.minus(Duration.ofMinutes(1))).map { at ->
                val fresh = InMemoryBalanceStore()
                val service = ProcessTransactionEventService(fresh, RecordingProcessingMetrics(), Clock.fixed(at, ZoneOffset.UTC), FutureTolerance(Duration.ofMinutes(5)))
                service.process(newer)
                service.process(older)
                fresh.peek(accountId)?.balance?.amount
            }

        assertEquals(setOf(BigDecimal("2.00")), results.toSet())
    }
}
