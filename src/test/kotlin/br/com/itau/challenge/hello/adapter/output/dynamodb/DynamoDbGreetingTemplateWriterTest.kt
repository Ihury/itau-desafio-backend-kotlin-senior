package br.com.itau.challenge.hello.adapter.output.dynamodb

import br.com.itau.challenge.hello.domain.model.GreetingTemplate
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.BDDMockito.given
import org.mockito.Mockito.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import kotlin.test.assertEquals

class DynamoDbGreetingTemplateWriterTest {

    @Test
    fun `should put the greeting template into the configured table`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.putItem(any(PutItemRequest::class.java))).willReturn(PutItemResponse.builder().build())
        val writer = DynamoDbGreetingTemplateWriter(client, "GreetingMessages")

        writer.save(GreetingTemplate(id = "k1", template = "Yo %s!"))

        val requestCaptor = ArgumentCaptor.forClass(PutItemRequest::class.java)
        verify(client).putItem(requestCaptor.capture())
        val request = requestCaptor.value
        assertEquals("GreetingMessages", request.tableName())
        assertEquals("k1", request.item()["id"]?.s())
        assertEquals("Yo %s!", request.item()["template"]?.s())
    }
}
