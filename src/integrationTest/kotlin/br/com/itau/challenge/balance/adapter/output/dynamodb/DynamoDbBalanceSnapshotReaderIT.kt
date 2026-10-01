package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.support.DynamoDbTestSupport
import br.com.itau.challenge.balance.support.DynamoDbTestSupport.item
import br.com.itau.challenge.balance.support.DynamoDbTestSupport.randomAccountId
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class DynamoDbBalanceSnapshotReaderIT {
    private lateinit var raw: DynamoDbClient
    private lateinit var readClient: DynamoDbClient
    private lateinit var reader: DynamoDbBalanceSnapshotReader
    private lateinit var accountId: String

    @BeforeEach
    fun setUp() {
        raw = DynamoDbTestSupport.rawClient()
        readClient = DynamoDbTestSupport.readClient()
        reader = DynamoDbBalanceSnapshotReader(readClient, DynamoDbTestSupport.tableName, true, SimpleMeterRegistry())
        accountId = randomAccountId()
    }

    @AfterEach
    fun tearDown() {
        raw.close()
        readClient.close()
    }

    private fun put(attributes: Map<String, AttributeValue>) {
        raw.putItem(PutItemRequest.builder().tableName(DynamoDbTestSupport.tableName).item(attributes).build())
    }

    @Test
    fun `database normalizes trailing zeros of the stored number and the reader still delivers the same value`() {
        put(item(accountId, balanceAmount = "183.10"))

        assertEquals("183.1", DynamoDbTestSupport.rawBalanceAmount(raw, accountId), "o DynamoDB normaliza 183.10 para 183.1")

        val snapshot = assertNotNull(reader.find(AccountId.parse(accountId)))
        assertEquals(0, BigDecimal("183.10").compareTo(snapshot.balance.amount))
        assertEquals(BigDecimal("183.10"), snapshot.balance.paddedToCurrencyScale())
    }

    @Test
    fun `thirty eight exact digits survive the round trip through the database`() {
        val exact = "12345678901234567890.123456789012345678"
        put(item(accountId, balanceAmount = exact))

        val snapshot = assertNotNull(reader.find(AccountId.parse(accountId)))

        assertEquals(BigDecimal(exact), snapshot.balance.amount)
    }

    @Test
    fun `thirty nine digits are rejected by the database itself which is why the domain enforces the ceiling`() {
        val failure =
            assertFailsWith<DynamoDbException> {
                put(item(accountId, balanceAmount = "123456789012345678901234567890123456789"))
            }

        val translated = DynamoDbExceptionTranslator.translateWriteFailure(failure)
        assertIs<BalanceStoreRejectedException>(translated, "o codigo de erro real do DynamoDB Local e reconhecido como ValidationException")
    }

    @Test
    fun `an absent account is read as null`() {
        assertNull(reader.find(AccountId.parse(accountId)))
    }

    @Test
    fun `sample item of the seed is read with its microsecond precision`() {
        val snapshot = assertNotNull(reader.find(AccountId.parse("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")), "rode `make db-up` (seed)")

        assertEquals(BigDecimal("183.12"), snapshot.balance.amount)
        assertEquals(1751749453433000L, snapshot.precedence.timestamp.micros)
        assertEquals("8e8ae808-b154-48b5-9f3e-553935cc4543", snapshot.precedence.transactionId.value)
    }

    @Test
    fun `an item with timestamps below the default plausibility minimums is read normally`() {
        put(item(accountId, lastTxTsMicros = TX_IN_1995_MICROS, accountCreatedAtMicros = ACCOUNT_CREATED_IN_1850_MICROS))

        val snapshot = assertNotNull(reader.find(AccountId.parse(accountId)))

        assertEquals(TX_IN_1995_MICROS, snapshot.precedence.timestamp.micros)
        assertEquals(ACCOUNT_CREATED_IN_1850_MICROS, snapshot.accountCreatedAt.micros)
    }

    @Test
    fun `microseconds of the last transaction are preserved`() {
        put(item(accountId, lastTxTsMicros = 1751749453433123L))

        assertEquals(1751749453433123L, assertNotNull(reader.find(AccountId.parse(accountId))).precedence.timestamp.micros)
    }

    @Test
    fun `an unreachable database is store unavailable and never absent`() {
        DynamoDbTestSupport.readClient(DynamoDbTestSupport.properties(endpoint = "http://localhost:1")).use { unreachable ->
            val failing = DynamoDbBalanceSnapshotReader(unreachable, DynamoDbTestSupport.tableName, true, SimpleMeterRegistry())

            val failure = assertFailsWith<BalanceStoreUnavailableException> { failing.find(AccountId.parse(accountId)) }

            assertEquals(StoreFailureCause.UNAVAILABLE, failure.failureCause)
        }
    }

    @Test
    fun `a missing table is a misconfiguration that stays transitory, with the real error code in the diagnostics`() {
        val failing = DynamoDbBalanceSnapshotReader(readClient, "TabelaQueNaoExiste", true, SimpleMeterRegistry())

        val failure = assertFailsWith<BalanceStoreUnavailableException> { failing.find(AccountId.parse(accountId)) }

        assertEquals(StoreFailureCause.MISCONFIGURED, failure.failureCause)
        val details = assertNotNull(failure.details)
        assertEquals("ResourceNotFoundException", details.errorCode)
        assertEquals(400, details.statusCode)
    }

    private companion object {
        const val TX_IN_1995_MICROS = 803_174_400_000_000L
        const val ACCOUNT_CREATED_IN_1850_MICROS = -3_155_760_000_000_000L
    }
}
