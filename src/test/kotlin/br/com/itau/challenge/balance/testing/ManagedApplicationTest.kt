package br.com.itau.challenge.balance.testing

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalManagementPort
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.bean.override.mockito.MockitoBean
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

// Subclasses compartilham UM contexto em cache: nao adicione @MockitoBean, propriedades ou anotacoes.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = ["management.server.port=0"])
@ActiveProfiles("test")
abstract class ManagedApplicationTest {
    @LocalServerPort
    protected var apiPort: Int = 0

    @LocalManagementPort
    protected var managementPort: Int = 0

    @MockitoBean(name = "dynamoDbReadClient")
    protected lateinit var readClient: DynamoDbClient

    @MockitoBean(name = "dynamoDbWriteClient")
    protected lateinit var writeClient: DynamoDbClient

    private val http = HttpClient.newHttpClient()

    protected fun api(
        path: String,
        vararg headers: String,
    ): HttpResponse<String> = call(apiPort, path, *headers)

    protected fun management(path: String): HttpResponse<String> = call(managementPort, path)

    private fun call(
        port: Int,
        path: String,
        vararg headers: String,
    ): HttpResponse<String> {
        val builder = HttpRequest.newBuilder(URI.create("http://localhost:$port$path")).GET()
        if (headers.isNotEmpty()) builder.headers(*headers)
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }
}
