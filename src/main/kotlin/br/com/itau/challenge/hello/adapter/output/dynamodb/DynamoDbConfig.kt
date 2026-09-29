package br.com.itau.challenge.hello.adapter.output.dynamodb

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI

@Configuration
class DynamoDbConfig {

    @Bean
    fun dynamoDbClient(
        @Value("\${dynamodb.endpoint}") endpoint: String,
        @Value("\${dynamodb.region}") region: String,
    ): DynamoDbClient =
        DynamoDbClient
            .builder()
            .endpointOverride(URI.create(endpoint))
            .region(Region.of(region))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")),
            ).build()
}
