package br.com.itau.challenge.balance.adapter.output.dynamodb

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.apache5.Apache5HttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI
import java.time.Duration

/**
 * Propriedades `dynamodb.*` (contracts/configuration.md; os defaults vivem no `application.yaml`). Pertencem ao adapter: nenhuma camada pode depender do
 * pacote `config`. [endpoint] em branco significa producao (endpoint AWS e cadeia padrao de credenciais).
 */
@ConfigurationProperties("dynamodb")
class DynamoDbClientProperties(
    val endpoint: String?,
    val region: String,
    val tableName: String,
    val connectTimeout: Duration,
    val acquireTimeout: Duration,
    val read: Read,
) {
    /** Cliente de leitura (API): unica camada de retry e o SDK; timeouts curtos para falhar rapido (SC-008). */
    class Read(
        val consistent: Boolean,
        val attemptTimeout: Duration,
        val callTimeout: Duration,
        val maxAttempts: Int,
        val maxConnections: Int,
    )
}

/** Valores efetivos do cliente HTTP (pool e timeouts explicitos, Constitution V). */
internal data class HttpSettings(
    val connectTimeout: Duration,
    val acquireTimeout: Duration,
    val socketTimeout: Duration,
    val maxConnections: Int,
)

@Configuration
@EnableConfigurationProperties(DynamoDbClientProperties::class)
class DynamoDbClientsConfig {
    /**
     * Cliente de leitura: retry `standard` do SDK com `maxAttempts` (1 retry), `apiCallAttemptTimeout` e
     * `apiCallTimeout` explicitos, pool proprio. Com [DynamoDbClientProperties.endpoint] usa `endpointOverride` e
     * credenciais estaticas locais; sem endpoint, a `DefaultCredentialsProvider` e o endpoint da regiao.
     */
    @Bean
    fun dynamoDbReadClient(properties: DynamoDbClientProperties): DynamoDbClient {
        val http = readHttpSettings(properties)
        val builder =
            DynamoDbClient
                .builder()
                .region(Region.of(properties.region))
                .credentialsProvider(credentialsOf(properties))
                .httpClientBuilder(
                    Apache5HttpClient
                        .builder()
                        .connectionTimeout(http.connectTimeout)
                        .connectionAcquisitionTimeout(http.acquireTimeout)
                        .socketTimeout(http.socketTimeout)
                        .maxConnections(http.maxConnections),
                ).overrideConfiguration(
                    ClientOverrideConfiguration
                        .builder()
                        .retryStrategy(AwsRetryStrategy.standardRetryStrategy().toBuilder().maxAttempts(properties.read.maxAttempts).build())
                        .apiCallAttemptTimeout(properties.read.attemptTimeout)
                        .apiCallTimeout(properties.read.callTimeout)
                        .build(),
                )
        endpointOf(properties)?.let { builder.endpointOverride(it) }
        return builder.build()
    }

    internal fun readHttpSettings(properties: DynamoDbClientProperties): HttpSettings =
        HttpSettings(
            connectTimeout = properties.connectTimeout,
            acquireTimeout = properties.acquireTimeout,
            socketTimeout = properties.read.attemptTimeout,
            maxConnections = properties.read.maxConnections,
        )

    internal fun endpointOf(properties: DynamoDbClientProperties): URI? = properties.endpoint?.takeIf { it.isNotBlank() }?.let(URI::create)

    private fun credentialsOf(properties: DynamoDbClientProperties): AwsCredentialsProvider =
        if (endpointOf(properties) != null) {
            StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local"))
        } else {
            DefaultCredentialsProvider.builder().build()
        }
}
