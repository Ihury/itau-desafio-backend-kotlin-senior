package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.domain.model.TransactionEventFixtures.transactionEvent
import br.com.itau.challenge.balance.domain.model.TransactionStatus
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import io.kotest.property.Arb
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.element
import io.kotest.property.arbitrary.int
import java.math.BigDecimal
import java.math.BigInteger
import java.util.Random
import java.util.UUID

/** Um armazenamento sob teste: o escritor e a leitura direta do snapshot vigente (fora do escritor). */
class StoreUnderTest(
    val writer: BalanceSnapshotWriter,
    val current: (AccountId) -> BalanceSnapshot?,
)

/**
 * Modelo compartilhado pela propriedade de convergencia (unitaria, sobre o fake) e pelos testes de integracao (DynamoDB
 * Local). O conteudo de um evento e DERIVADO DETERMINISTICAMENTE da sua chave `(conta, timestamp, transactionId)`: a mesma
 * chave sempre produz o mesmo conteudo (dono, situacao, saldo, moeda, criacao), como faz um autorizador correto. Duplicatas
 * com conteudo divergente sao a anomalia `conflicting_duplicate` e nao entram aqui (nao sao convergentes por definicao).
 *
 * O oraculo ordena por `(timestamp, transactionId minusculo)` com `String.compareTo` e NUNCA usa `UUID.compareTo`.
 */
object ConvergenceModel {
    const val BASE_TIMESTAMP_MICROS = 1751749453433000L

    /** Intervalo pequeno de timestamps (0..3 us de deslocamento) para forcar empates de `timestamp`. */
    const val TIMESTAMP_SPAN = 4

    val ACCOUNTS: List<String> = listOf("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975", "0a7e3e1c-2a55-4b58-a8f4-4c1b6a1f3d10")

    /**
     * Conjunto fixo de `transactionId`. Inclui pares em que `java.util.UUID.compareTo` (compara `long` com sinal) diverge da
     * ordem textual (a do DynamoDB): `ffffffff-...` x `00000000-...` e `7fffffff-...` x `80000000-...`.
     */
    val TRANSACTION_IDS: List<String> =
        listOf(
            "00000000-0000-4000-8000-000000000001",
            "ffffffff-ffff-4fff-8fff-ffffffffff01",
            "7fffffff-ffff-4fff-8fff-ffffffffff02",
            "80000000-0000-4000-8000-000000000003",
            "8e8ae808-b154-48b5-9f3e-553935cc4543",
            "0a7e3e1c-2a55-4b58-a8f4-4c1b6a1f3d10",
        )

    /** Existe ao menos um par de ids em que `UUID.compareTo` discorda da ordem textual (a armadilha esta exercitada). */
    fun hasUuidCompareToDivergence(): Boolean =
        TRANSACTION_IDS.any { a ->
            TRANSACTION_IDS.any { b -> Integer.signum(UUID.fromString(a).compareTo(UUID.fromString(b))) != Integer.signum(a.compareTo(b)) }
        }

    /**
     * Um evento gerado: [account] indexa [ACCOUNTS], [tsOffset] desloca o `timestamp` base, [txIndex] escolhe o `transactionId`
     * e [upperCaseTx] entrega o mesmo id em maiusculas (o dominio normaliza para minusculas).
     */
    data class EventSpec(
        val account: Int,
        val tsOffset: Int,
        val txIndex: Int,
        val upperCaseTx: Boolean,
        val accountOverride: String? = null,
    ) {
        /** Conta do evento: a de [ACCOUNTS] ou, nos testes de integracao (tabela compartilhada), uma conta aleatoria. */
        val accountId: String get() = accountOverride ?: ACCOUNTS[account]

        /** Mesmo evento numa outra conta (o conteudo e rederivado da nova chave). */
        fun withAccount(id: String): EventSpec = copy(accountOverride = id)
        val timestampMicros: Long get() = BASE_TIMESTAMP_MICROS + tsOffset
        val transactionId: String get() = TRANSACTION_IDS[txIndex]

        /** Chave de precedencia textual: `(timestamp, transactionId minusculo)`. */
        val key: Pair<Long, String> get() = timestampMicros to transactionId

        fun toEvent(): TransactionEvent {
            val content = contentOf(this)
            return transactionEvent(
                transactionId = if (upperCaseTx) transactionId.uppercase() else transactionId,
                timestampMicros = timestampMicros,
                transactionStatus = content.transactionStatus,
                accountId = accountId,
                ownerId = content.ownerId,
                accountStatus = content.accountStatus,
                balanceAmount = content.balanceAmount,
                balanceCurrency = content.currency,
                accountCreatedAtMicros = content.accountCreatedAtMicros,
            )
        }

        fun toSnapshot(): BalanceSnapshot = BalanceSnapshot.from(toEvent())
    }

    /** Conteudo derivado da chave: nao depende de [EventSpec.upperCaseTx], so de `(conta, timestamp, transactionId)`. */
    data class Content(
        val ownerId: String,
        val accountStatus: AccountStatus,
        val transactionStatus: TransactionStatus,
        val balanceAmount: String,
        val currency: String,
        val accountCreatedAtMicros: Long,
    )

    fun contentOf(spec: EventSpec): Content {
        val mixed = Random("${spec.accountId}|${spec.timestampMicros}|${spec.transactionId}".hashCode().toLong()).nextLong()
        val bits = Random(mixed)
        return Content(
            ownerId = UUID(mixed, mixed.rotateLeft(17) xor 0x5DEECE66DL).toString(),
            accountStatus = if (bits.nextBoolean()) AccountStatus.ENABLED else AccountStatus.DISABLED,
            transactionStatus = if (bits.nextBoolean()) TransactionStatus.APPROVED else TransactionStatus.DECLINED,
            balanceAmount = BigDecimal(BigInteger.valueOf(bits.nextInt(1_000_000_000).toLong()), bits.nextInt(4)).toPlainString(),
            currency = if (bits.nextBoolean()) "BRL" else "USD",
            accountCreatedAtMicros = 1_500_000_000_000_000L + bits.nextInt(1_000_000),
        )
    }

    /** Oraculo: o evento de maior `(timestamp, transactionId minusculo)` por `String.compareTo`, sem `UUID.compareTo`. */
    fun winnerOf(specs: List<EventSpec>): EventSpec? =
        specs.maxWithOrNull(compareBy<EventSpec> { it.timestampMicros }.thenBy { it.transactionId })

    /** Gerador de eventos para [accounts] contas (1 = uma conta so). */
    fun eventSpecs(accounts: Int): Arb<EventSpec> =
        Arb.bind(
            Arb.int(0 until accounts),
            Arb.int(0 until TIMESTAMP_SPAN),
            Arb.element(TRANSACTION_IDS.indices.toList()),
            Arb.boolean(),
        ) { account, tsOffset, txIndex, upper -> EventSpec(account, tsOffset, txIndex, upper) }
}
