package br.com.itau.challenge.hello.adapter.output.dynamodb

import br.com.itau.challenge.hello.port.output.GreetingTemplateProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

private const val TEMPLATE_ATTRIBUTE = "template"

@Component
class DynamoDbGreetingTemplateProvider(
    private val dynamoDbClient: DynamoDbClient,
    @Value("\${dynamodb.table-name}") private val tableName: String,
) : GreetingTemplateProvider {

    override fun randomTemplate(): String {
        val request =
            ScanRequest
                .builder()
                .tableName(tableName)
                .projectionExpression(TEMPLATE_ATTRIBUTE)
                .build()

        val templates =
            dynamoDbClient
                .scan(request)
                .items()
                .map { it.getValue(TEMPLATE_ATTRIBUTE).s() }

        check(templates.isNotEmpty()) { "No greeting templates found in table '$tableName'" }

        return templates.random()
    }
}
