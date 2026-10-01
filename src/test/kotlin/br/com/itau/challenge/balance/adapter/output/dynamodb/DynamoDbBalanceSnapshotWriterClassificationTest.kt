package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.HIGHEST_TRANSACTION_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import br.com.itau.challenge.balance.testing.numberAttr
import br.com.itau.challenge.balance.testing.stringAttr
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class DynamoDbBalanceSnapshotWriterClassificationTest : DynamoDbWriterFixture() {
    @Test
    fun `a failed condition against a greater current item is obsolete and never an error`() {
        stubConditionFailedWithCurrentItem(itemOf(transactionEvent(timestampMicros = snapshot.precedence.timestamp.micros + 1, balanceAmount = "999.00")))

        assertEquals(ApplyResult.Obsolete, writer.applyIfNewer(snapshot))
    }

    @Test
    fun `a failed condition against the same timestamp and a greater transaction id is obsolete`() {
        stubConditionFailedWithCurrentItem(itemOf(transactionEvent(transactionId = HIGHEST_TRANSACTION_ID)))

        assertEquals(ApplyResult.Obsolete, writer.applyIfNewer(snapshot))
    }

    @Test
    fun `a failed condition against the same key and content is a plain duplicate`() {
        stubConditionFailedWithCurrentItem(itemOf(transactionEvent()))

        assertEquals(ApplyResult.Duplicate(conflicting = false), writer.applyIfNewer(snapshot))
    }

    @Test
    fun `a duplicate whose stored balance has a larger scale is not conflicting`() {
        val stored = itemOf(transactionEvent()).toMutableMap()
        stored["balanceAmount"] = numberAttr("183.120")
        stubConditionFailedWithCurrentItem(stored)

        assertEquals(ApplyResult.Duplicate(conflicting = false), writer.applyIfNewer(snapshot))
    }

    @Test
    fun `a duplicate whose stored balance was normalized by the database to a smaller scale is not conflicting`() {
        val normalized = itemOf(transactionEvent(balanceAmount = "183.10")).toMutableMap()
        normalized["balanceAmount"] = numberAttr("183.1")
        stubConditionFailedWithCurrentItem(normalized)

        assertEquals(ApplyResult.Duplicate(conflicting = false), writer.applyIfNewer(BalanceSnapshot.from(transactionEvent(balanceAmount = "183.10"))))
    }

    @Test
    fun `a duplicate with divergent content is a conflicting duplicate for each compared field`() {
        val divergent =
            mapOf(
                "ownerId" to transactionEvent(ownerId = "dddddddd-f4af-4cd2-b298-a449e614349a"),
                "accountStatus" to transactionEvent(accountStatus = AccountStatus.DISABLED),
                "balanceCurrency" to transactionEvent(balanceCurrency = "USD"),
                "balanceAmount" to transactionEvent(balanceAmount = "999.99"),
            )
        divergent.forEach { (field, event) ->
            stubConditionFailedWithCurrentItem(itemOf(event))

            assertEquals(ApplyResult.Duplicate(conflicting = true), writer.applyIfNewer(snapshot), field)
        }
    }

    @Test
    fun `there is no read before the write and none when the failed condition carries the old item`() {
        stubUpdateSucceeds()
        writer.applyIfNewer(snapshot)
        stubConditionFailedWithCurrentItem(itemOf(transactionEvent()))
        writer.applyIfNewer(snapshot)

        verify(client, never()).getItem(any(GetItemRequest::class.java))
        verify(client, times(2)).updateItem(any(UpdateItemRequest::class.java))
    }

    @Test
    fun `without the old item a single consistent get item classifies the outcome`() {
        stubConditionFailedWithoutCurrentItem()
        stubGetItem(itemOf(transactionEvent(timestampMicros = snapshot.precedence.timestamp.micros + 5)))

        assertEquals(ApplyResult.Obsolete, writer.applyIfNewer(snapshot))

        val captor = ArgumentCaptor.forClass(GetItemRequest::class.java)
        verify(client, times(1)).getItem(captor.capture())
        assertTrue(captor.value.consistentRead())
        assertEquals("AccountBalances", captor.value.tableName())
        assertEquals(BalanceItemMapper.keyOf(snapshot.accountId), captor.value.key())
    }

    @Test
    fun `the fallback read classifies the same content as a plain duplicate`() {
        stubConditionFailedWithoutCurrentItem()
        stubGetItem(itemOf(transactionEvent()))

        assertEquals(ApplyResult.Duplicate(conflicting = false), writer.applyIfNewer(snapshot))
    }

    @Test
    fun `the fallback read classifies divergent content as a conflicting duplicate`() {
        stubConditionFailedWithoutCurrentItem()
        stubGetItem(itemOf(transactionEvent(balanceAmount = "1.00")))

        assertEquals(ApplyResult.Duplicate(conflicting = true), writer.applyIfNewer(snapshot))
    }

    @Test
    fun `when the item is missing even in the fallback read the failure is transient and the event is redelivered`() {
        stubConditionFailedWithoutCurrentItem()
        stubGetItem(null)

        val thrown = assertFailsWith<BalanceStoreUnavailableException> { writer.applyIfNewer(snapshot) }

        assertEquals(StoreFailureCause.UNAVAILABLE, thrown.failureCause)
    }

    @Test
    fun `a current item with a lower precedence than the event contradicts the failed condition and is not transient`() {
        stubConditionFailedWithoutCurrentItem()
        stubGetItem(itemOf(transactionEvent(timestampMicros = snapshot.precedence.timestamp.micros - 1, balanceAmount = "5555.55")))

        val thrown: Throwable = assertFailsWith<IllegalStateException> { writer.applyIfNewer(snapshot) }

        assertTrue(thrown !is BalanceStoreUnavailableException, "retentar para sempre bloquearia a particao")
        assertTrue("5555.55" !in thrown.message.orEmpty() && "183.12" !in thrown.message.orEmpty(), "sem valores na mensagem")
    }

    @Test
    fun `the same contradiction carried by the old item of the failed condition is also an internal failure`() {
        stubConditionFailedWithCurrentItem(itemOf(transactionEvent(timestampMicros = snapshot.precedence.timestamp.micros - 1, balanceAmount = "5555.55")))

        val thrown = assertFailsWith<IllegalStateException> { writer.applyIfNewer(snapshot) }

        assertTrue("5555.55" !in thrown.message.orEmpty(), "sem valores na mensagem")
        verify(client, never()).getItem(any(GetItemRequest::class.java))
    }

    @Test
    fun `a failure of the fallback read is translated and never swallowed`() {
        stubConditionFailedWithoutCurrentItem()
        doThrow(ProvisionedThroughputExceededException.builder().message("x").build()).`when`(client).getItem(any(GetItemRequest::class.java))

        val thrown = assertFailsWith<BalanceStoreUnavailableException> { writer.applyIfNewer(snapshot) }

        assertEquals(StoreFailureCause.THROTTLED, thrown.failureCause)
    }

    @Test
    fun `a current item without the precedence attributes is an internal failure and never a classification`() {
        stubConditionFailedWithCurrentItem(mapOf("pk" to stringAttr("ACCOUNT#x")))

        assertFailsWith<IllegalStateException> { writer.applyIfNewer(snapshot) }
    }

    @Test
    fun `an unreadable current item fails without leaking balances or owners in the message`() {
        stubConditionFailedWithCurrentItem(mapOf("lastTxTsMicros" to numberAttr("abc")))

        val thrown = assertFailsWith<IllegalStateException> { writer.applyIfNewer(snapshot) }

        assertTrue("183.12" !in thrown.message.orEmpty() && "315e3cfe" !in thrown.message.orEmpty())
    }
}
