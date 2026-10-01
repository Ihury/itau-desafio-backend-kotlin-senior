package br.com.itau.challenge.balance.testing

import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.listener.BackOffHandler
import org.springframework.kafka.listener.MessageListenerContainer

class RecordingBackOffHandler : BackOffHandler {
    val intervals = mutableListOf<Long>()

    override fun onNextBackOff(
        container: MessageListenerContainer?,
        exception: Exception?,
        nextBackOff: Long,
    ) {
        intervals += nextBackOff
    }

    override fun onNextBackOff(
        container: MessageListenerContainer,
        partition: TopicPartition,
        nextBackOff: Long,
    ) {
        intervals += nextBackOff
    }
}
