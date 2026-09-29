package br.com.itau.challenge.hello.adapter.output.dynamodb

import br.com.itau.challenge.hello.domain.model.GreetingTemplate
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import java.net.URI
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Exercises the DynamoDB adapters against a real, running DynamoDB Local instance
 * (see `make db-up`). Not part of `./gradlew test`/`check` since it needs live
 * infrastructure; run explicitly with `./gradlew integrationTest` or `make integration-test`.
 */
class DynamoDbGreetingTemplateIntegrationTest {

    private val tableName = System.getenv("GREETING_TABLE_NAME") ?: "GreetingMessages"

    private val dynamoDbClient: DynamoDbClient =
        DynamoDbClient
            .builder()
            .endpointOverride(URI.create(System.getenv("DYNAMODB_ENDPOINT") ?: "http://localhost:8000"))
            .region(Region.of(System.getenv("DYNAMODB_REGION") ?: "us-east-1"))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
            .build()

    private val writer = DynamoDbGreetingTemplateWriter(dynamoDbClient, tableName)
    private val provider = DynamoDbGreetingTemplateProvider(dynamoDbClient, tableName)

    private lateinit var testId: String

    @BeforeEach
    fun setUp() {
        testId = "integration-test-${UUID.randomUUID()}"
    }

    @AfterEach
    fun tearDown() {
        dynamoDbClient.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(tableName)
                .key(mapOf("id" to AttributeValue.builder().s(testId).build()))
                .build(),
        )
    }

    @Test
    fun `should persist a greeting template that can be read back from the real table`() {
        val template = GreetingTemplate(id = testId, template = "Yo %s! From the integration test!")

        writer.save(template)

        val response =
            dynamoDbClient.getItem(
                GetItemRequest
                    .builder()
                    .tableName(tableName)
                    .key(mapOf("id" to AttributeValue.builder().s(testId).build()))
                    .build(),
            )

        assertTrue(response.hasItem())
        assertEquals(testId, response.item()["id"]?.s())
        assertEquals("Yo %s! From the integration test!", response.item()["template"]?.s())
    }

    @Test
    fun `should read a template back from the real table via a live scan`() {
        writer.save(GreetingTemplate(id = testId, template = "Yo %s! From the integration test!"))

        val template = provider.randomTemplate()

        assertTrue(template.isNotBlank())
        assertTrue(template.contains("%s"))
    }
}
