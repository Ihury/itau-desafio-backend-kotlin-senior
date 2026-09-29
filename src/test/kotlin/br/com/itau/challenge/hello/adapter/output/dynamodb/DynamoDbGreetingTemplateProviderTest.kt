package br.com.itau.challenge.hello.adapter.output.dynamodb

import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.BDDMockito.given
import org.mockito.Mockito.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class DynamoDbGreetingTemplateProviderTest {

    private fun itemOf(template: String): Map<String, AttributeValue> =
        mapOf("template" to AttributeValue.builder().s(template).build())

    @Test
    fun `should scan the configured table and return one of its templates`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.scan(any(ScanRequest::class.java))).willReturn(
            ScanResponse.builder().items(itemOf("Hello, %s!")).build(),
        )
        val provider = DynamoDbGreetingTemplateProvider(client, "GreetingMessages")

        val template = provider.randomTemplate()

        assertEquals("Hello, %s!", template)
    }

    @Test
    fun `should scan using the configured table name`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.scan(any(ScanRequest::class.java))).willReturn(
            ScanResponse.builder().items(itemOf("Hi there, %s!")).build(),
        )
        val provider = DynamoDbGreetingTemplateProvider(client, "CustomTableName")

        provider.randomTemplate()

        val requestCaptor = ArgumentCaptor.forClass(ScanRequest::class.java)
        verify(client).scan(requestCaptor.capture())
        assertEquals("CustomTableName", requestCaptor.value.tableName())
    }

    @Test
    fun `should eventually return more than one distinct template when several exist`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.scan(any(ScanRequest::class.java))).willReturn(
            ScanResponse
                .builder()
                .items(itemOf("Hello, %s!"), itemOf("Hi there, %s!"), itemOf("Greetings, %s!"))
                .build(),
        )
        val provider = DynamoDbGreetingTemplateProvider(client, "GreetingMessages")

        val observedTemplates = (1..50).map { provider.randomTemplate() }.toSet()

        assertTrue(observedTemplates.size > 1)
    }

    @Test
    fun `should fail fast when the table has no templates`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.scan(any(ScanRequest::class.java))).willReturn(ScanResponse.builder().items(emptyList()).build())
        val provider = DynamoDbGreetingTemplateProvider(client, "GreetingMessages")

        assertFailsWith<IllegalStateException> { provider.randomTemplate() }
    }
}
