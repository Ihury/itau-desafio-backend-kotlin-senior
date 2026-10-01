package br.com.itau.challenge.balance.support

import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientProperties
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbClientsConfig
import br.com.itau.challenge.balance.testing.numberAttr
import br.com.itau.challenge.balance.testing.stringAttr
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes
import java.math.BigDecimal
import java.net.URI
import java.time.Duration
import java.util.UUID

object DynamoDbTestSupport {
    private const val BATCH_GET_LIMIT = 100

    val endpoint: String = System.getenv("DYNAMODB_ENDPOINT")?.takeIf { it.isNotBlank() } ?: "http://localhost:8000"
    val tableName: String = System.getenv("BALANCE_TABLE_NAME")?.takeIf { it.isNotBlank() } ?: "AccountBalances"

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

    fun readClient(properties: DynamoDbClientProperties = properties()): DynamoDbClient =
        DynamoDbClientsConfig().dynamoDbReadClient(properties)

    fun writeClient(properties: DynamoDbClientProperties = properties()): DynamoDbClient =
        DynamoDbClientsConfig().dynamoDbWriteClient(properties)

    fun randomAccountId(): String = UUID.randomUUID().toString()

    @Suppress("LongParameterList")
    fun item(
        accountId: String,
        balanceAmount: String = "183.12",
        balanceCurrency: String = "BRL",
        accountStatus: String = "ENABLED",
        ownerId: String = EventPayloads.DEFAULT_OWNER,
        lastTxTsMicros: Long = EventPayloads.BASE_TIMESTAMP_MICROS,
        lastTxId: String = EventPayloads.DEFAULT_TRANSACTION_ID,
        accountCreatedAtMicros: Long = EventPayloads.DEFAULT_ACCOUNT_CREATED_AT_MICROS,
    ): Map<String, AttributeValue> =
        mapOf(
            "pk" to stringAttr("ACCOUNT#$accountId"),
            "sk" to stringAttr("BALANCE"),
            "schemaVersion" to numberAttr("1"),
            "ownerId" to stringAttr(ownerId),
            "accountStatus" to stringAttr(accountStatus),
            "balanceAmount" to numberAttr(balanceAmount),
            "balanceCurrency" to stringAttr(balanceCurrency),
            "accountCreatedAtMicros" to numberAttr(accountCreatedAtMicros.toString()),
            "lastTxTsMicros" to numberAttr(lastTxTsMicros.toString()),
            "lastTxId" to stringAttr(lastTxId),
        )

    fun key(accountId: String): Map<String, AttributeValue> = mapOf("pk" to stringAttr("ACCOUNT#$accountId"), "sk" to stringAttr("BALANCE"))

    fun deleteAccount(
        client: DynamoDbClient,
        accountId: String,
    ) {
        client.deleteItem(DeleteItemRequest.builder().tableName(tableName).key(key(accountId)).build())
    }

    fun rawBalanceAmount(
        client: DynamoDbClient,
        accountId: String,
    ): String =
        client
            .getItem(GetItemRequest.builder().tableName(tableName).key(key(accountId)).consistentRead(true).build())
            .item()
            .getValue("balanceAmount")
            .n()

    fun storedBalances(
        client: DynamoDbClient,
        accounts: List<String>,
    ): Map<String, BigDecimal> =
        accounts
            .chunked(BATCH_GET_LIMIT)
            .flatMap { chunk ->
                val keys = KeysAndAttributes.builder().keys(chunk.map { key(it) }).consistentRead(true).build()
                client.batchGetItem(BatchGetItemRequest.builder().requestItems(mapOf(tableName to keys)).build()).responses()[tableName].orEmpty()
            }.associate { it.getValue("pk").s().removePrefix("ACCOUNT#") to BigDecimal(it.getValue("balanceAmount").n()) }
}
