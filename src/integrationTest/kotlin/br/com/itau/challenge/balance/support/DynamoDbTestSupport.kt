package br.com.itau.challenge.balance.support

import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientProperties
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientsConfig
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.net.URI
import java.time.Duration
import java.util.UUID

/** Apoio dos testes de integracao contra o DynamoDB Local do compose (`make db-up`). */
object DynamoDbTestSupport {
    val endpoint: String = System.getenv("DYNAMODB_ENDPOINT")?.takeIf { it.isNotBlank() } ?: "http://localhost:8000"
    val tableName: String = System.getenv("BALANCE_TABLE_NAME")?.takeIf { it.isNotBlank() } ?: "AccountBalances"

    /** Cliente cru (sem retry curto nem circuit breaker) para preparar e inspecionar itens diretamente. */
    fun rawClient(): DynamoDbClient =
        DynamoDbClient
            .builder()
            .endpointOverride(URI.create(endpoint))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
            .build()

    fun properties(
        endpoint: String = this.endpoint,
        consistent: Boolean = true,
    ) = DynamoDbClientProperties(
        endpoint = endpoint,
        region = "us-east-1",
        tableName = tableName,
        connectTimeout = Duration.ofMillis(300),
        acquireTimeout = Duration.ofMillis(300),
        read =
            DynamoDbClientProperties.Read(
                consistent = consistent,
                attemptTimeout = Duration.ofMillis(600),
                callTimeout = Duration.ofMillis(1500),
                maxAttempts = 2,
                maxConnections = 100,
            ),
        write =
            DynamoDbClientProperties.Write(
                attemptTimeout = Duration.ofSeconds(2),
                callTimeout = Duration.ofSeconds(2),
                maxConnections = 50,
            ),
    )

    /** Cliente de leitura da aplicacao, configurado como em producao (retry standard, timeouts explicitos). */
    fun readClient(properties: DynamoDbClientProperties = properties()): DynamoDbClient =
        DynamoDbClientsConfig().dynamoDbReadClient(properties)

    /** Cliente de escrita da aplicacao, configurado como em producao (uma tentativa, timeouts explicitos). */
    fun writeClient(properties: DynamoDbClientProperties = properties()): DynamoDbClient =
        DynamoDbClientsConfig().dynamoDbWriteClient(properties)

    fun randomAccountId(): String = UUID.randomUUID().toString()

    /** Item do snapshot escrito com valores literais (independe do mapper de producao). */
    @Suppress("LongParameterList")
    fun item(
        accountId: String,
        balanceAmount: String = "183.12",
        balanceCurrency: String = "BRL",
        accountStatus: String = "ENABLED",
        ownerId: String = "315e3cfe-f4af-4cd2-b298-a449e614349a",
        lastTxTsMicros: Long = 1751749453433000L,
        lastTxId: String = "8e8ae808-b154-48b5-9f3e-553935cc4543",
        accountCreatedAtMicros: Long = 1634874339000000L,
    ): Map<String, AttributeValue> =
        mapOf(
            "pk" to s("ACCOUNT#$accountId"),
            "sk" to s("BALANCE"),
            "schemaVersion" to n("1"),
            "ownerId" to s(ownerId),
            "accountStatus" to s(accountStatus),
            "balanceAmount" to n(balanceAmount),
            "balanceCurrency" to s(balanceCurrency),
            "accountCreatedAtMicros" to n(accountCreatedAtMicros.toString()),
            "lastTxTsMicros" to n(lastTxTsMicros.toString()),
            "lastTxId" to s(lastTxId),
        )

    fun key(accountId: String): Map<String, AttributeValue> = mapOf("pk" to s("ACCOUNT#$accountId"), "sk" to s("BALANCE"))

    private fun s(value: String): AttributeValue = AttributeValue.builder().s(value).build()

    private fun n(value: String): AttributeValue = AttributeValue.builder().n(value).build()
}
