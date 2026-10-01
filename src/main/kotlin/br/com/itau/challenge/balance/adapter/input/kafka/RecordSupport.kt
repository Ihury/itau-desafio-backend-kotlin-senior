package br.com.itau.challenge.balance.adapter.input.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.MDC

internal fun ConsumerRecord<*, *>.coordinates(): String = "${topic()}-${partition()}@${offset()}"

internal inline fun <T> withMdc(
    vararg entries: Pair<String, String>,
    block: () -> T,
): T {
    entries.forEach { (key, value) -> MDC.put(key, value) }
    try {
        return block()
    } finally {
        entries.forEach { (key, _) -> MDC.remove(key) }
    }
}
