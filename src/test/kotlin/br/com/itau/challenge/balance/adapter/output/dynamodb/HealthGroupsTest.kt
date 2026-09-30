package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.testing.ManagedApplicationTest
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.health.actuate.endpoint.HealthEndpointGroups
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse
import software.amazon.awssdk.services.dynamodb.model.TableDescription
import software.amazon.awssdk.services.dynamodb.model.TableStatus
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Grupos de saude na porta de gerenciamento (FR-033, Constitution VII): `liveness` e `readiness` refletem so o estado do proprio
 * processo e permanecem 200 com o DynamoDB fora (a instancia nao sai de rotacao); a saude da dependencia vive no grupo
 * `dependencies` e no gauge `balance.dependency.up`.
 */
class HealthGroupsTest : ManagedApplicationTest() {
    @Autowired
    private lateinit var groups: HealthEndpointGroups

    private fun dynamoIsUp() {
        val table = TableDescription.builder().tableName("AccountBalances").tableStatus(TableStatus.ACTIVE).build()
        doReturn(DescribeTableResponse.builder().table(table).build()).`when`(readClient).describeTable(any(DescribeTableRequest::class.java))
    }

    private fun dynamoIsDown() {
        doThrow(SdkClientException.builder().message("Unable to execute HTTP request: connect to dynamodb:8000 failed").build())
            .`when`(readClient)
            .describeTable(any(DescribeTableRequest::class.java))
    }

    private fun gauge(): Double {
        val line = management("/actuator/prometheus").body().lines().single { it.startsWith("balance_dependency_up{") }
        return line.substringAfterLast(' ').toDouble()
    }

    @Test
    fun `the groups are liveness, readiness and dependencies and only the process state is in the probes`() {
        assertTrue(groups.names.containsAll(setOf("liveness", "readiness", "dependencies")), "grupos: ${groups.names}")
        val readiness = assertNotNull(groups.get("readiness"))
        val liveness = assertNotNull(groups.get("liveness"))
        val dependencies = assertNotNull(groups.get("dependencies"))

        assertTrue(readiness.isMember("readinessState"))
        assertTrue(!readiness.isMember("dynamoDb"), "o DynamoDB nao pode estar na readiness")
        assertTrue(liveness.isMember("livenessState"))
        assertTrue(!liveness.isMember("dynamoDb"), "o DynamoDB nao pode estar na liveness")
        assertTrue(dependencies.isMember("dynamoDb"))
        assertTrue(!dependencies.isMember("readinessState") && !dependencies.isMember("livenessState"))
    }

    @Test
    fun `with dynamodb down dependencies is 503 while liveness and readiness stay 200 and after it returns everything is up`() {
        dynamoIsDown()

        // o resultado do probe fica em cache por 5 s (pode vir de outro teste): espera a janela renovar
        await.atMost(Duration.ofSeconds(8)).untilAsserted { assertEquals(503, management("/actuator/health/dependencies").statusCode()) }
        val dependencies = management("/actuator/health/dependencies")
        assertEquals("""{"status":"DOWN"}""", dependencies.body(), "show-details=never: nada alem do status")
        assertEquals(200, management("/actuator/health/liveness").statusCode())
        assertEquals("""{"status":"UP"}""", management("/actuator/health/liveness").body())
        assertEquals(200, management("/actuator/health/readiness").statusCode())
        assertEquals("""{"status":"UP"}""", management("/actuator/health/readiness").body())
        assertEquals(0.0, gauge())
        assertEquals(503, management("/actuator/health").statusCode(), "a raiz agrega as dependencias e nao serve de sonda")

        dynamoIsUp()

        await.atMost(Duration.ofSeconds(8)).untilAsserted { assertEquals(200, management("/actuator/health/dependencies").statusCode()) }
        assertEquals("""{"status":"UP"}""", management("/actuator/health/dependencies").body())
        assertEquals(1.0, gauge())
        assertEquals(200, management("/actuator/health/readiness").statusCode())
        assertEquals(200, management("/actuator/health/liveness").statusCode())
        assertEquals(200, management("/actuator/health").statusCode())
    }

    @Test
    fun `health is not served on the api port`() {
        listOf("/actuator/health", "/actuator/health/liveness", "/actuator/health/readiness", "/actuator/health/dependencies").forEach { path ->
            assertEquals(404, api(path).statusCode(), "$path nao pode responder na porta da API")
        }
    }
}
