package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.support.DynamoDbTestSupport
import br.com.itau.challenge.balance.testing.ConvergenceProperty
import br.com.itau.challenge.balance.testing.StoreUnderTest
import io.kotest.common.ExperimentalKotest
import io.kotest.property.PropTestConfig
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import java.util.Collections
import kotlin.test.assertEquals

/**
 * A MESMA propriedade de convergencia do teste unitario (`ConvergencePropertyTest`), em versao reduzida (50 iteracoes,
 * `seed` fixa), contra o DynamoDB Local real: o escritor de producao e o banco arbitram permutacoes, duplicacoes, empates,
 * `DECLINED` e `DISABLED`. Cada entrega usa contas aleatorias novas (a tabela e compartilhada entre execucoes).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@OptIn(ExperimentalKotest::class)
class ConvergencePropertyIT {
    private companion object {
        const val SEED = 20260929L
        const val ITERATIONS = 50
    }

    private val writeClient: DynamoDbClient = DynamoDbTestSupport.writeClient()
    private val readerClient: DynamoDbClient = DynamoDbTestSupport.rawClient()
    private val writer = DynamoDbBalanceSnapshotWriter(writeClient, DynamoDbTestSupport.tableName, SimpleMeterRegistry())
    private val reader = DynamoDbBalanceSnapshotReader(readerClient, DynamoDbTestSupport.tableName, true, SimpleMeterRegistry())
    private val touched = Collections.synchronizedSet(mutableSetOf<String>())

    @AfterAll
    fun cleanUp() {
        touched.forEach { readerClient.deleteItem(DeleteItemRequest.builder().tableName(DynamoDbTestSupport.tableName).key(DynamoDbTestSupport.key(it)).build()) }
        writeClient.close()
        readerClient.close()
    }

    private fun scenario(): ConvergenceProperty.Scenario {
        val remap = ConvergenceProperty.randomAccounts()
        return ConvergenceProperty.Scenario(
            store = StoreUnderTest(writer) { accountId: AccountId -> reader.find(accountId) },
            remap = { spec -> remap(spec).also { touched += it.accountId } },
        )
    }

    @Test
    fun `one account converges against the real database under permutation and duplication`() {
        val context = ConvergenceProperty.run(PropTestConfig(seed = SEED, iterations = ITERATIONS), accounts = 1, newScenario = ::scenario)

        assertEquals(ITERATIONS, context.attempts())
    }

    @Test
    fun `two interleaved accounts converge independently against the real database`() {
        val context = ConvergenceProperty.run(PropTestConfig(seed = SEED, iterations = ITERATIONS), accounts = 2, newScenario = ::scenario)

        assertEquals(ITERATIONS, context.attempts())
    }
}
