package br.com.itau.challenge.balance.adapter.output.dynamodb

import org.junit.jupiter.api.Test
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import java.net.URI
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DynamoDbClientsConfigTest {
    private val config = DynamoDbClientsConfig()

    private fun properties(endpoint: String? = "http://localhost:8000") =
        DynamoDbClientProperties(
            endpoint = endpoint,
            region = "us-east-1",
            tableName = "AccountBalances",
            connectTimeout = Duration.ofMillis(300),
            acquireTimeout = Duration.ofMillis(300),
            read =
                DynamoDbClientProperties.Read(
                    consistent = true,
                    attemptTimeout = Duration.ofMillis(600),
                    callTimeout = Duration.ofMillis(1500),
                    maxAttempts = 2,
                    maxConnections = 100,
                ),
        )

    @Test
    fun `read client uses the standard retry strategy with two attempts and explicit timeouts`() {
        config.dynamoDbReadClient(properties()).use { client ->
            val override = client.serviceClientConfiguration().overrideConfiguration()

            assertEquals(2, override.retryStrategy().orElseThrow().maxAttempts())
            assertEquals(Duration.ofMillis(600), override.apiCallAttemptTimeout().orElseThrow())
            assertEquals(Duration.ofMillis(1500), override.apiCallTimeout().orElseThrow())
        }
    }

    @Test
    fun `read client http settings follow the properties`() {
        val settings = config.readHttpSettings(properties())

        assertEquals(Duration.ofMillis(300), settings.connectTimeout)
        assertEquals(Duration.ofMillis(300), settings.acquireTimeout)
        assertEquals(Duration.ofMillis(600), settings.socketTimeout)
        assertEquals(100, settings.maxConnections)
    }

    @Test
    fun `with an endpoint the client uses the override and static local credentials`() {
        config.dynamoDbReadClient(properties("http://dynamodb:8000")).use { client ->
            val configuration = client.serviceClientConfiguration()

            assertEquals(URI.create("http://dynamodb:8000"), configuration.endpointOverride().orElseThrow())
            assertEquals(Region.US_EAST_1, configuration.region())
            val credentials = configuration.credentialsProvider().resolveIdentity().join()
            assertEquals(AwsBasicCredentials.create("local", "local").accessKeyId(), credentials.accessKeyId())
        }
    }

    @Test
    fun `without an endpoint the client uses the default credentials chain and the aws endpoint`() {
        listOf(null, "", "  ").forEach { blank ->
            config.dynamoDbReadClient(properties(blank)).use { client ->
                val configuration = client.serviceClientConfiguration()

                assertTrue(configuration.endpointOverride().isEmpty, "endpoint em branco nao sobrescreve")
                assertIs<DefaultCredentialsProvider>(configuration.credentialsProvider())
            }
        }
    }

    @Test
    fun `no endpoint override object is left when the property is blank`() {
        assertNull(config.endpointOf(properties(null)))
        assertNull(config.endpointOf(properties("")))
        assertEquals(URI.create("http://localhost:8000"), config.endpointOf(properties()))
    }
}
