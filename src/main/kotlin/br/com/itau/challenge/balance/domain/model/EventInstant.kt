package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import java.time.Instant

/**
 * Instante de um evento em microssegundos desde a epoca (`Long`), sem perda de precisao. O minimo aceito depende do
 * papel do campo: [transactionTimestamp] (entra na precedencia; detecta segundos/milissegundos) e [accountCreatedAt]
 * (contas anteriores a 2000 sao legitimas, e valores anteriores a 1970 sao negativos). O maximo (agora + tolerancia)
 * precisa de relogio e e verificado na camada `application`. Um valor ja persistido volta por [fromPersisted], sem checagem.
 */
@JvmInline
value class EventInstant private constructor(
    private val epochMicros: Long,
) : Comparable<EventInstant> {
    val micros: Long get() = epochMicros

    /** Converte sem perda; `floorDiv/floorMod` sao obrigatorios para microssegundos negativos. */
    fun toInstant(): Instant =
        Instant.ofEpochSecond(
            Math.floorDiv(epochMicros, MICROS_PER_SECOND),
            Math.floorMod(epochMicros, MICROS_PER_SECOND) * NANOS_PER_MICRO,
        )

    override fun compareTo(other: EventInstant): Int = epochMicros.compareTo(other.epochMicros)

    override fun toString(): String = epochMicros.toString()

    companion object {
        private const val MICROS_PER_SECOND = 1_000_000L
        private const val NANOS_PER_MICRO = 1_000L

        /** `transaction.timestamp` >= 2000-01-01T00:00:00Z. */
        val TRANSACTION_MINIMUM: Instant = Instant.parse("2000-01-01T00:00:00Z")

        /** `account.created_at` >= 1900-01-01T00:00:00Z. */
        val ACCOUNT_CREATED_AT_MINIMUM: Instant = Instant.parse("1900-01-01T00:00:00Z")

        fun transactionTimestamp(
            micros: Long,
            minimum: Instant = TRANSACTION_MINIMUM,
        ): EventInstant = of(micros, minimum)

        fun accountCreatedAt(
            micros: Long,
            minimum: Instant = ACCOUNT_CREATED_AT_MINIMUM,
        ): EventInstant = of(micros, minimum)

        /**
         * Reconstroi um instante que JA foi validado na escrita, sem checar faixa. Os minimos de plausibilidade sao configuraveis
         * (`BALANCE_MIN_*`) e valem para o evento que ENTRA; um snapshot persistido e confiavel e nao pode virar erro de leitura por
         * uma configuracao diferente da vigente quando foi gravado. Uso exclusivo de adapters de saida ao reidratar dado proprio.
         */
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
