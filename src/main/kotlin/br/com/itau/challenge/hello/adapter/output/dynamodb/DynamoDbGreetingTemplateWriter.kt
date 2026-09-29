package br.com.itau.challenge.hello.adapter.output.dynamodb

import br.com.itau.challenge.hello.domain.model.GreetingTemplate
import br.com.itau.challenge.hello.port.output.GreetingTemplateRepository
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest

private const val ID_ATTRIBUTE = "id"
private const val TEMPLATE_ATTRIBUTE = "template"

@Component
class DynamoDbGreetingTemplateWriter(
    private val dynamoDbClient: DynamoDbClient,
    @Value("\${dynamodb.table-name}") private val tableName: String,
) : GreetingTemplateRepository {

    override fun save(greetingTemplate: GreetingTemplate) {
        val request =
            PutItemRequest
                .builder()
                .tableName(tableName)
                .item(
                    mapOf(
                        ID_ATTRIBUTE to AttributeValue.builder().s(greetingTemplate.id).build(),
                        TEMPLATE_ATTRIBUTE to AttributeValue.builder().s(greetingTemplate.template).build(),
                    ),
                ).build()

        dynamoDbClient.putItem(request)
    }
}
