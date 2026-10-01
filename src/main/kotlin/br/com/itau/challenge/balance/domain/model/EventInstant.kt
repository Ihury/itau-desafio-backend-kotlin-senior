package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import java.time.Instant

@JvmInline
value class EventInstant private constructor(
    val micros: Long,
) : Comparable<EventInstant> {
    fun toInstant(): Instant =
        Instant.ofEpochSecond(
            Math.floorDiv(micros, MICROS_PER_SECOND),
            Math.floorMod(micros, MICROS_PER_SECOND) * NANOS_PER_MICRO,
        )

    fun isAfter(instant: Instant): Boolean = micros > toMicros(instant)

    override fun compareTo(other: EventInstant): Int = micros.compareTo(other.micros)

    override fun toString(): String = micros.toString()

    companion object {
        private const val MICROS_PER_SECOND = 1_000_000L
        private const val NANOS_PER_MICRO = 1_000L

        val DEFAULT_TRANSACTION_MINIMUM: Instant = Instant.parse("2000-01-01T00:00:00Z")

        val DEFAULT_ACCOUNT_CREATED_AT_MINIMUM: Instant = Instant.parse("1900-01-01T00:00:00Z")

        fun transactionTimestamp(
            micros: Long,
            minimum: Instant = DEFAULT_TRANSACTION_MINIMUM,
        ): EventInstant = of(micros, minimum)

        fun accountCreatedAt(
            micros: Long,
            minimum: Instant = DEFAULT_ACCOUNT_CREATED_AT_MINIMUM,
        ): EventInstant = of(micros, minimum)

        fun fromPersisted(micros: Long): EventInstant = EventInstant(micros)

        private fun of(
            micros: Long,
            minimum: Instant,
        ): EventInstant {
            if (micros < toMicros(minimum)) throw InvalidEventException(RejectionReason.INVALID_TIMESTAMP)
            return EventInstant(micros)
        }

        private fun toMicros(instant: Instant): Long =
            Math.addExact(
                Math.multiplyExact(instant.epochSecond, MICROS_PER_SECOND),
                (instant.nano / NANOS_PER_MICRO),
            )
    }
}
