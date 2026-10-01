package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.support.DynamoDbTestSupport
import br.com.itau.challenge.balance.support.DynamoDbTestSupport.randomAccountId
import br.com.itau.challenge.balance.testing.BalanceSnapshotWriterContract
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DynamoDbBalanceSnapshotWriterContractIT : BalanceSnapshotWriterContract() {
    private val writeClient: DynamoDbClient = DynamoDbTestSupport.writeClient()
    private val raw: DynamoDbClient = DynamoDbTestSupport.rawClient()
    private val reader = DynamoDbBalanceSnapshotReader(raw, DynamoDbTestSupport.tableName, true, SimpleMeterRegistry())

    override val writer: BalanceSnapshotWriter = DynamoDbBalanceSnapshotWriter(writeClient, DynamoDbTestSupport.tableName, SimpleMeterRegistry())

    override fun currentStoredSnapshotOf(accountId: AccountId): BalanceSnapshot? = reader.find(accountId)

    @AfterAll
    fun closeClients() {
        writeClient.close()
        raw.close()
    }

    @Test
    fun `a balance whose scale the database may normalize is still numerically identical`() {
        val account = randomAccountId()
        val event = transactionEvent(accountId = account, balanceAmount = "183.10")

        assertEquals(ApplyResult.Applied, writer.applyIfNewer(BalanceSnapshot.from(event)))

        val rawAmount = DynamoDbTestSupport.rawBalanceAmount(raw, account)
        assertEquals(
            0,
            BigDecimal("183.10").compareTo(BigDecimal(rawAmount)),
            "valor armazenado identico, com ou sem normalizacao de escala (DynamoDB Local 3.3.0 preserva 183.10 via UpdateItem; o contrato e o valor)",
        )
        val stored = assertNotNull(currentStoredSnapshotOf(AccountId.parse(account)))
        assertEquals(0, BigDecimal("183.10").compareTo(stored.balance.amount))
        assertEquals(BalanceSnapshot.from(event), stored, "igualdade de Money por valor numerico")
        assertEquals(BigDecimal("183.10"), stored.balance.paddedToCurrencyScale())
    }
}
