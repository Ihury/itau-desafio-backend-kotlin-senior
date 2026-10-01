package br.com.itau.challenge.balance.adapter.output.dynamodb

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
import software.amazon.awssdk.retries.api.RetryStrategy
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.time.Duration

@Configuration
@EnableConfigurationProperties(DynamoDbClientProperties::class)
class DynamoDbClientsConfig {
    @Bean
    fun dynamoDbReadClient(properties: DynamoDbClientProperties): DynamoDbClient =
        buildClient(
            properties = properties,
            http = HttpSettings.forRead(properties),
            retryStrategy = AwsRetryStrategy.standardRetryStrategy().toBuilder().maxAttempts(properties.read.maxAttempts).build(),
            attemptTimeout = properties.read.attemptTimeout,
            callTimeout = properties.read.callTimeout,
        )

    @Bean
    fun dynamoDbWriteClient(properties: DynamoDbClientProperties): DynamoDbClient =
        buildClient(
            properties = properties,
            http = HttpSettings.forWrite(properties),
            retryStrategy = AwsRetryStrategy.doNotRetry(),
            attemptTimeout = properties.write.attemptTimeout,
            callTimeout = properties.write.callTimeout,
        )

    private fun buildClient(
        properties: DynamoDbClientProperties,
        http: HttpSettings,
        retryStrategy: RetryStrategy,
        attemptTimeout: Duration,
        callTimeout: Duration,
    ): DynamoDbClient {
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
                        .retryStrategy(retryStrategy)
                        .apiCallAttemptTimeout(attemptTimeout)
                        .apiCallTimeout(callTimeout)
                        .build(),
                )
        properties.endpointUri?.let { builder.endpointOverride(it) }
        return builder.build()
    }

    private fun credentialsOf(properties: DynamoDbClientProperties): AwsCredentialsProvider =
        if (properties.endpointUri != null) LOCAL_CREDENTIALS else DefaultCredentialsProvider.builder().build()

    private companion object {
        val LOCAL_CREDENTIALS: AwsCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local"))
    }
}
