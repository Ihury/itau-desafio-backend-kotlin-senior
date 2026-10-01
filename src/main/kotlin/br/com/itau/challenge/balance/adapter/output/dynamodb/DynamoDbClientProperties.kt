package br.com.itau.challenge.balance.adapter.output.dynamodb

import org.springframework.boot.context.properties.ConfigurationProperties
import java.net.URI
import java.time.Duration

@ConfigurationProperties("dynamodb")
class DynamoDbClientProperties(
    val endpoint: String?,
    val region: String,
    val tableName: String,
    val connectTimeout: Duration,
    val acquireTimeout: Duration,
    val read: Read,
    val write: Write,
) {
    val endpointUri: URI? get() = endpoint?.takeIf { it.isNotBlank() }?.let(URI::create)

    class Read(
        val consistent: Boolean,
        val attemptTimeout: Duration,
        val callTimeout: Duration,
        val maxAttempts: Int,
        val maxConnections: Int,
    )

    class Write(
        val attemptTimeout: Duration,
        val callTimeout: Duration,
        val maxConnections: Int,
    )
}

internal data class HttpSettings(
    val connectTimeout: Duration,
    val acquireTimeout: Duration,
    val socketTimeout: Duration,
    val maxConnections: Int,
) {
    companion object {
        fun forRead(properties: DynamoDbClientProperties) =
            HttpSettings(
                connectTimeout = properties.connectTimeout,
                acquireTimeout = properties.acquireTimeout,
                socketTimeout = properties.read.attemptTimeout,
                maxConnections = properties.read.maxConnections,
            )

        fun forWrite(properties: DynamoDbClientProperties) =
            HttpSettings(
                connectTimeout = properties.connectTimeout,
                acquireTimeout = properties.acquireTimeout,
                socketTimeout = properties.write.attemptTimeout,
                maxConnections = properties.write.maxConnections,
            )
    }
}
