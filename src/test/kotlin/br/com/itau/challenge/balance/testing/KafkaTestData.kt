package br.com.itau.challenge.balance.testing

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.mock
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.ListenerExecutionFailedException

const val TRANSACTIONS_TOPIC = "transacoes-financeiras-processadas"
const val DEAD_LETTER_TOPIC = "transacoes-financeiras-processadas.DLT"

fun aRecord(
    value: ByteArray?,
    partition: Int = 7,
    offset: Long = 41L,
    key: ByteArray? = null,
    topic: String = TRANSACTIONS_TOPIC,
): ConsumerRecord<Any, Any> = ConsumerRecord<Any, Any>(topic, partition, offset, key, value)

fun listenerFailed(cause: Exception): ListenerExecutionFailedException = ListenerExecutionFailedException("Listener failed", cause)

fun isRedeliverySignal(failure: RuntimeException): Boolean = failure.javaClass.simpleName == "RecordInRetryException"

@Suppress("UNCHECKED_CAST")
fun mockKafkaOperations(): KafkaOperations<ByteArray, ByteArray> = mock(KafkaOperations::class.java) as KafkaOperations<ByteArray, ByteArray>

fun anyProducerRecord(): ProducerRecord<ByteArray, ByteArray> {
    any(ProducerRecord::class.java)
    return nullOf()
}

@Suppress("UNCHECKED_CAST")
private fun <T> nullOf(): T = null as T
