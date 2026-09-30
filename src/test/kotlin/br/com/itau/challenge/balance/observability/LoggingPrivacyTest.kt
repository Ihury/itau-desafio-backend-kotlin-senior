package br.com.itau.challenge.balance.observability

import br.com.itau.challenge.balance.adapter.input.kafka.BackOffProperties
import br.com.itau.challenge.balance.adapter.input.kafka.DeadLetterConfig
import br.com.itau.challenge.balance.adapter.input.kafka.TransactionEventListener
import br.com.itau.challenge.balance.adapter.output.dynamodb.BalanceItemMapper
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import br.com.itau.challenge.balance.testing.ManagedApplicationTest
import br.com.itau.challenge.balance.testing.RecordingProcessingMetrics
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.BackOffHandler
import org.springframework.kafka.listener.ListenerExecutionFailedException
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.kafka.support.SendResult
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.io.PrintWriter
import java.io.StringWriter
import java.time.Clock
import java.time.Duration
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Os logs saem em JSON (um objeto por linha), com `correlationId`, `accountId` e `transactionId` como
 * chaves de topo, e NUNCA carregam saldo, titular, payload nem mensagem de parser. Valores sentinela sao usados em todos os
 * caminhos (aplicado, duplicado divergente, payload malformado, campos invalidos, falha do armazenamento, item corrompido e o
 * error handler do consumer, inclusive o que o proprio Spring Kafka emite).
 */
@ExtendWith(OutputCaptureExtension::class)
class LoggingPrivacyTest : ManagedApplicationTest() {
    @Autowired
    private lateinit var listener: TransactionEventListener

    private val json = JsonMapper.builder().build()

    private val balanceSentinel = "98765.43"
    private val ownerSentinel = "0b5e1c2d-aaaa-4bbb-8ccc-1234567890ab"
    private val textSentinel = "SEGREDO-XYZ-123"
    private val accountId = "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975"
    private val transactionId = "8e8ae808-b154-48b5-9f3e-553935cc4543"

    private fun payload(
        balance: String = balanceSentinel,
        owner: String = ownerSentinel,
        currency: String = "BRL",
    ) = """{"transaction":{"id":"$transactionId","type":"CREDIT","amount":97.07,"currency":"$currency","status":"APPROVED",""" +
        """"timestamp":1751641364589998},"account":{"id":"$accountId","owner":"$owner","created_at":1634874339000000,""" +
        """"status":"ENABLED","balance":{"amount":$balance,"currency":"BRL"}}}"""

    private fun record(
        value: String,
        offset: Long,
    ) = ConsumerRecord<ByteArray?, ByteArray?>("transacoes-financeiras-processadas", 7, offset, null, value.toByteArray())

    private fun stackTraceOf(failure: Throwable): String = StringWriter().also { failure.printStackTrace(PrintWriter(it)) }.toString()

    /**
     * Linhas emitidas a partir de [from] (o tamanho da saida quando o corpo do teste comecou): o que a JVM escreve antes disso
     * (aviso de auto-anexo do Mockito, por exemplo) nao e log da aplicacao e nao entra na verificacao.
     */
    private fun lines(
        output: CapturedOutput,
        from: Int,
    ): List<String> = output.all.substring(from).lines().filter { it.isNotBlank() }

    private fun parsed(line: String): JsonNode = json.readTree(line)

    private fun JsonNode.text(field: String): String? = this[field]?.asString()

    private fun assertNoSensitiveData(
        text: String,
        where: String,
    ) {
        listOf(balanceSentinel, "98765", ownerSentinel, textSentinel, "97.07", "1751641364589998").forEach { sentinel ->
            assertTrue(sentinel !in text, "'$sentinel' vazou para $where")
        }
    }

    @Test
    fun `every line is json and ingestion and api lines carry the correlation, account and transaction and nothing sensitive`(output: CapturedOutput) {
        val from = output.all.length
        // ingestao: aplicado (INFO) e duplicado com conteudo divergente (WARN); tudo pelo listener real
        listener.onMessage(record(payload(), offset = 10))
        val divergent =
            BalanceItemMapper.toItem(BalanceSnapshot.from(transactionEvent(transactionId = transactionId, timestampMicros = 1751641364589998L, balanceAmount = "1.00")))
        doThrow(ConditionalCheckFailedException.builder().message("The conditional request failed").item(divergent).build())
            .`when`(writeClient)
            .updateItem(any(UpdateItemRequest::class.java))
        listener.onMessage(record(payload(), offset = 11))
        // rejeitados: payload malformado com sentinela, moeda invalida com sentinela e titular invalido
        val failures =
            listOf(
                """{"transaction": $textSentinel""" to 12L,
                payload(currency = textSentinel) to 13L,
                payload(owner = textSentinel) to 14L,
            ).map { (value, offset) -> assertFailsWith<InvalidEventException> { listener.onMessage(record(value, offset)) } }
        failures.forEach { assertNoSensitiveData(stackTraceOf(it), "a excecao do parser") }

        // API: falha do armazenamento (503, WARN) e item corrompido (500, ERROR com pilha), com correlacao informada pelo cliente
        doThrow(SdkClientException.builder().message("connect to dynamodb:8000 failed").build()).`when`(readClient).getItem(any(GetItemRequest::class.java))
        assertEquals(503, api("/balances/$accountId", "X-Correlation-Id", "teste-123").statusCode())
        val corrupted = BalanceItemMapper.toItem(BalanceSnapshot.from(transactionEvent())).toMutableMap()
        corrupted["accountStatus"] = AttributeValue.builder().s("SUSPENDED").build()
        doReturn(GetItemResponse.builder().item(corrupted).build()).`when`(readClient).getItem(any(GetItemRequest::class.java))
        val corruptedAccount = "aaaaaaaa-1111-4222-8333-bbbbbbbbbbbb"
        assertEquals(500, api("/balances/$corruptedAccount", "X-Correlation-Id", "teste-456").statusCode())

        val emittedLines = lines(output, from)
        assertTrue(emittedLines.isNotEmpty(), "nenhum log capturado")
        val records = emittedLines.map { line -> runCatching { parsed(line) }.getOrElse { throw AssertionError("linha que nao e JSON: $line") } }
        emittedLines.forEach { assertTrue(it.startsWith("{") && it.endsWith("}"), "linha nao e um objeto JSON: $it") }
        assertNoSensitiveData(emittedLines.joinToString("\n"), "a saida de log")

        val applied = records.first { it.text("message")?.startsWith("event applied") == true }
        assertEquals("transacoes-financeiras-processadas-7@10", applied.text("correlationId"))
        assertEquals(accountId, applied.text("accountId"))
        assertEquals(transactionId, applied.text("transactionId"))
        val conflicting = records.first { it.text("message")?.startsWith("conflicting duplicate event") == true }
        assertEquals("transacoes-financeiras-processadas-7@11", conflicting.text("correlationId"))
        assertEquals(accountId, conflicting.text("accountId"))
        assertEquals(transactionId, conflicting.text("transactionId"))

        val unavailable = records.first { it.text("message")?.startsWith("balance store unavailable") == true }
        assertEquals("teste-123", unavailable.text("correlationId"))
        assertEquals(accountId, unavailable.text("accountId"))
        val cannotMap = records.first { it.text("message")?.startsWith("balance item cannot be mapped") == true }
        assertEquals("teste-456", cannotMap.text("correlationId"))
        assertEquals(corruptedAccount, cannotMap.text("accountId"))
        val internal = records.first { it.text("message")?.startsWith("unexpected error while handling request") == true }
        assertEquals("teste-456", internal.text("correlationId"))
        assertNotNull(internal.text("accountId"))
    }

    @Test
    fun `the consumer error handler and spring kafka never log payload values, whatever the failure class`(output: CapturedOutput) {
        val from = output.all.length
        @Suppress("UNCHECKED_CAST")
        val template = mock(KafkaOperations::class.java) as KafkaOperations<ByteArray, ByteArray>
        doAnswer { invocation ->
            val outbound = invocation.getArgument<ProducerRecord<ByteArray, ByteArray>>(0)
            CompletableFuture.completedFuture(SendResult(outbound, RecordMetadata(TopicPartition(outbound.topic(), 1), 0L, 0, 0L, 0, 0)))
        }.`when`(template).send(anyProducerRecord())
        val noOpBackOffHandler =
            object : BackOffHandler {
                override fun onNextBackOff(
                    container: MessageListenerContainer?,
                    exception: Exception?,
                    nextBackOff: Long,
                ) = Unit

                override fun onNextBackOff(
                    container: MessageListenerContainer,
                    partition: TopicPartition,
                    nextBackOff: Long,
                ) = Unit
            }
        val handler =
            DeadLetterConfig().deadLetterErrorHandler(
                template,
                "transacoes-financeiras-processadas.DLT",
                Duration.ofMillis(300),
                Clock.systemUTC(),
                RecordingProcessingMetrics(),
                BackOffProperties(initialMs = 10, maxMs = 20, jitterMs = 0),
                noOpBackOffHandler,
            )
        val consumer = mock(Consumer::class.java)
        val container = mock(MessageListenerContainer::class.java)

        fun deliver(
            value: String,
            offset: Long,
            cause: Exception,
            times: Int,
        ) {
            val record = ConsumerRecord<Any, Any>("transacoes-financeiras-processadas", 7, offset, null, value.toByteArray())
            repeat(times) {
                try {
                    handler.handleRemaining(ListenerExecutionFailedException("Listener failed", cause), listOf(record), consumer, container)
                } catch (signal: RuntimeException) {
                    if (signal.javaClass.simpleName != "RecordInRetryException") throw signal
                }
            }
        }

        // permanente (parser com sentinelas), nao classificada (falha interna) e transiente (armazenamento), com o payload sentinela
        val invalid = assertFailsWith<InvalidEventException> { listener.onMessage(record("""{"transaction": $textSentinel""", 20)) }
        deliver("""{"transaction": $textSentinel""", 20, invalid, times = 1)
        deliver(payload(), 21, IllegalStateException("current balance item contradicts the failed condition"), times = 3)
        deliver(payload(), 22, BalanceStoreUnavailableException(StoreFailureCause.TIMEOUT, SdkClientException.builder().message("connect to dynamodb:8000 failed").build()), times = 2)

        val emitted = lines(output, from)
        assertTrue(emitted.isNotEmpty(), "o handler deveria ter registrado algo")
        emitted.forEach { line -> runCatching { parsed(line) }.getOrElse { throw AssertionError("linha que nao e JSON: $line") } }
        assertNoSensitiveData(emitted.joinToString("\n"), "o log do error handler e do Spring Kafka")
    }

    /** Matcher do Mockito que devolve um valor do tipo esperado (o `any()` devolve `null`, que o Kotlin recusa em tipo nao nulo). */
    private fun anyProducerRecord():ProducerRecord<ByteArray, ByteArray> {
        any(ProducerRecord::class.java)
        return nullOf()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> nullOf(): T = null as T
}
