package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import br.com.itau.challenge.balance.domain.model.TransactionStatus
import br.com.itau.challenge.balance.testing.InMemoryBalanceStore
import br.com.itau.challenge.balance.testing.RecordingProcessingMetrics
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class ProcessTransactionEventServiceTest {
    private val store = InMemoryBalanceStore()
    private val metrics = RecordingProcessingMetrics()
    private val service = ProcessTransactionEventService(store, metrics)

    private val accountA = "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975"
    private val accountB = "0a7e3e1c-2a55-4b58-a8f4-4c1b6a1f3d10"

    private val logs = ListAppender<ILoggingEvent>()
    private val serviceLogger = LoggerFactory.getLogger(ProcessTransactionEventService::class.java) as Logger
    private val previousLevel = serviceLogger.level

    @BeforeEach
    fun captureLogs() {
        serviceLogger.level = Level.DEBUG
        logs.start()
        serviceLogger.addAppender(logs)
    }

    @AfterEach
    fun releaseLogs() {
        serviceLogger.detachAppender(logs)
        serviceLogger.level = previousLevel
        logs.stop()
    }

    private fun current(account: String = accountA) = assertNotNull(store.current(AccountId.parse(account)))

    @Test
    fun `an event for an account without snapshot creates it with the data of the event`() {
        val result = service.process(transactionEvent(balanceAmount = "183.12", timestampMicros = 1751749453433000L))

        assertEquals(ApplyResult.Applied, result)
        val snapshot = current()
        assertEquals(BigDecimal("183.12"), snapshot.balance.amount)
        assertEquals("BRL", snapshot.balance.currency.value)
        assertEquals("315e3cfe-f4af-4cd2-b298-a449e614349a", snapshot.ownerId.value)
        assertEquals(AccountStatus.ENABLED, snapshot.status)
        assertEquals(1751749453433000L, snapshot.precedence.timestamp.micros)
        assertEquals(listOf("applied"), metrics.outcomes)
    }

    @Test
    fun `a newer event replaces balance, owner and instant`() {
        service.process(transactionEvent(timestampMicros = 1751749453433000L, balanceAmount = "100.00"))

        val result =
            service.process(
                transactionEvent(
                    transactionId = "9f9ae808-b154-48b5-9f3e-553935cc4543",
                    timestampMicros = 1751749453433001L,
                    balanceAmount = "250.50",
                    ownerId = "aaaaaaaa-f4af-4cd2-b298-a449e614349a",
                ),
            )

        assertEquals(ApplyResult.Applied, result)
        val snapshot = current()
        assertEquals(BigDecimal("250.50"), snapshot.balance.amount)
        assertEquals("aaaaaaaa-f4af-4cd2-b298-a449e614349a", snapshot.ownerId.value)
        assertEquals(1751749453433001L, snapshot.precedence.timestamp.micros)
        assertEquals("9f9ae808-b154-48b5-9f3e-553935cc4543", snapshot.precedence.transactionId.value)
        assertEquals(listOf("applied", "applied"), metrics.outcomes)
    }

    @Test
    fun `an older event is obsolete and leaves the snapshot untouched`() {
        service.process(transactionEvent(timestampMicros = 1751749453433000L, balanceAmount = "100.00"))
        val before = current()

        val result = service.process(transactionEvent(transactionId = "00000000-0000-4000-8000-000000000001", timestampMicros = 1751749453432999L, balanceAmount = "1.00"))

        assertEquals(ApplyResult.Obsolete, result)
        assertEquals(before, current())
        assertEquals(listOf("applied", "obsolete"), metrics.outcomes)
    }

    @Test
    fun `the same event delivered twice is a duplicate`() {
        val event = transactionEvent()
        service.process(event)

        val result = service.process(event)

        assertEquals(ApplyResult.Duplicate(conflicting = false), result)
        assertEquals(listOf("applied", "duplicate"), metrics.outcomes)
    }

    @Test
    fun `the same key with divergent content is a conflicting duplicate and the first one prevails`() {
        service.process(transactionEvent(balanceAmount = "183.12"))

        val result = service.process(transactionEvent(balanceAmount = "999.99"))

        assertEquals(ApplyResult.Duplicate(conflicting = true), result)
        assertEquals(BigDecimal("183.12"), current().balance.amount)
        assertEquals(listOf("applied", "duplicate(conflicting)"), metrics.outcomes)
    }

    @Test
    fun `events of different accounts interleaved do not interfere`() {
        service.process(transactionEvent(accountId = accountA, balanceAmount = "10.00", timestampMicros = 1751749453433000L))
        service.process(transactionEvent(accountId = accountB, balanceAmount = "20.00", timestampMicros = 1751749453000000L))
        service.process(transactionEvent(accountId = accountA, balanceAmount = "11.00", timestampMicros = 1751749454000000L, transactionId = "11111111-b154-48b5-9f3e-553935cc4543"))

        assertEquals(BigDecimal("11.00"), current(accountA).balance.amount)
        assertEquals(BigDecimal("20.00"), current(accountB).balance.amount)
        assertEquals(listOf("applied", "applied", "applied"), metrics.outcomes)
    }

    @Test
    fun `a newer declined event replaces the snapshot and an older declined one is obsolete`() {
        service.process(transactionEvent(timestampMicros = 1751749453433000L, balanceAmount = "100.00"))

        val newer = service.process(transactionEvent(transactionId = "22222222-b154-48b5-9f3e-553935cc4543", timestampMicros = 1751749453433500L, transactionStatus = TransactionStatus.DECLINED, balanceAmount = "77.00"))
        val older = service.process(transactionEvent(transactionId = "33333333-b154-48b5-9f3e-553935cc4543", timestampMicros = 1751749453400000L, transactionStatus = TransactionStatus.DECLINED, balanceAmount = "5.00"))

        assertEquals(ApplyResult.Applied, newer)
        assertEquals(ApplyResult.Obsolete, older)
        assertEquals(BigDecimal("77.00"), current().balance.amount)
    }

    @Test
    fun `every field of the snapshot comes from the same event`() {
        service.process(transactionEvent(timestampMicros = 1751749453433000L, balanceAmount = "100.00", accountStatus = AccountStatus.ENABLED, ownerId = "aaaaaaaa-f4af-4cd2-b298-a449e614349a"))
        service.process(
            transactionEvent(
                transactionId = "44444444-b154-48b5-9f3e-553935cc4543",
                timestampMicros = 1751749453433002L,
                balanceAmount = "300.00",
                accountStatus = AccountStatus.DISABLED,
                ownerId = "bbbbbbbb-f4af-4cd2-b298-a449e614349a",
                accountCreatedAtMicros = 1600000000000000L,
            ),
        )
        // um evento mais antigo chegando depois nao pode misturar nenhum campo
        service.process(transactionEvent(transactionId = "55555555-b154-48b5-9f3e-553935cc4543", timestampMicros = 1751749453433001L, balanceAmount = "1.00"))

        val snapshot = current()
        assertEquals(BigDecimal("300.00"), snapshot.balance.amount)
        assertEquals(AccountStatus.DISABLED, snapshot.status)
        assertEquals("bbbbbbbb-f4af-4cd2-b298-a449e614349a", snapshot.ownerId.value)
        assertEquals(1600000000000000L, snapshot.accountCreatedAt.micros)
        assertEquals("44444444-b154-48b5-9f3e-553935cc4543", snapshot.precedence.transactionId.value)
    }

    @Test
    fun `the balance currency prevails over the transaction currency without conversion or rejection`() {
        val result = service.process(transactionEvent(transactionCurrency = "USD", transactionAmount = "10.00", balanceCurrency = "BRL", balanceAmount = "183.12"))

        assertEquals(ApplyResult.Applied, result)
        assertEquals("BRL", current().balance.currency.value)
        assertEquals(BigDecimal("183.12"), current().balance.amount)
    }

    @Test
    fun `an unavailable store propagates untouched and no outcome is counted`() {
        val failure = BalanceStoreUnavailableException(StoreFailureCause.THROTTLED)
        store.failWritesWith(failure)

        val thrown = assertFailsWith<BalanceStoreUnavailableException> { service.process(transactionEvent()) }

        assertSame(failure, thrown)
        assertEquals(emptyList(), metrics.outcomes)
        assertNull(store.current(AccountId.parse(accountA)))
    }

    @Test
    fun `a store rejection propagates untouched and no outcome is counted`() {
        val failure = BalanceStoreRejectedException()
        store.failWritesWith(failure)

        assertSame(failure, assertFailsWith<BalanceStoreRejectedException> { service.process(transactionEvent()) })
        assertEquals(emptyList(), metrics.outcomes)
    }

    @Test
    fun `once the store recovers the same event is applied`() {
        store.failWritesWith(BalanceStoreUnavailableException(StoreFailureCause.UNAVAILABLE))
        assertFailsWith<BalanceStoreUnavailableException> { service.process(transactionEvent()) }
        store.failWritesWith(null)

        assertEquals(ApplyResult.Applied, service.process(transactionEvent()))
        assertEquals(listOf("applied"), metrics.outcomes)
    }

    @Test
    fun `a plain duplicate counts one duplicate with no anomaly and no warning`() {
        val event = transactionEvent()
        service.process(event)

        service.process(event)

        assertEquals(listOf("applied", "duplicate"), metrics.outcomes)
        assertEquals(emptyList(), logs.list.filter { it.level == Level.WARN })
    }

    @Test
    fun `a conflicting duplicate warns with account and transaction ids and without balance or owner`() {
        service.process(transactionEvent(balanceAmount = "183.12"))

        service.process(transactionEvent(balanceAmount = "999.99", ownerId = "dddddddd-f4af-4cd2-b298-a449e614349a"))

        assertEquals(listOf("applied", "duplicate(conflicting)"), metrics.outcomes)
        val warnings = logs.list.filter { it.level == Level.WARN }
        assertEquals(1, warnings.size)
        val text = warnings.single().formattedMessage
        assertTrue(accountA in text && "8e8ae808-b154-48b5-9f3e-553935cc4543" in text, text)
        assertTrue("183.12" !in text && "999.99" !in text && "dddddddd" !in text && "315e3cfe" !in text, text)
    }

    @Test
    fun `an obsolete event counts one obsolete without error or warning`() {
        service.process(transactionEvent(timestampMicros = 1751749453433000L))

        service.process(transactionEvent(transactionId = "00000000-0000-4000-8000-000000000001", timestampMicros = 1751749453432999L))

        assertEquals(listOf("applied", "obsolete"), metrics.outcomes)
        assertEquals(emptyList(), logs.list.filter { it.level == Level.WARN || it.level == Level.ERROR })
    }
}
