package br.com.itau.challenge.balance.testing

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.domain.model.TransactionStatus
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import io.kotest.property.Arb
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.element
import io.kotest.property.arbitrary.int
import java.math.BigDecimal
import java.math.BigInteger
import java.util.Random
import java.util.UUID

class StoreUnderTest(
    val writer: BalanceSnapshotWriter,
    val currentStoredSnapshot: (AccountId) -> BalanceSnapshot?,
)

object ConvergenceModel {
    const val BASE_TIMESTAMP_MICROS = 1751749453433000L

    const val TIE_PRONE_TIMESTAMP_SPAN = 4

    val ACCOUNTS: List<String> = listOf("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975", "0a7e3e1c-2a55-4b58-a8f4-4c1b6a1f3d10")

    val TRANSACTION_IDS_WITH_UUID_COMPARE_TRAPS: List<String> =
        listOf(
            "00000000-0000-4000-8000-000000000001",
            "ffffffff-ffff-4fff-8fff-ffffffffff01",
            "7fffffff-ffff-4fff-8fff-ffffffffff02",
            "80000000-0000-4000-8000-000000000003",
            "8e8ae808-b154-48b5-9f3e-553935cc4543",
            "0a7e3e1c-2a55-4b58-a8f4-4c1b6a1f3d10",
        )

    fun hasUuidCompareToDivergence(): Boolean =
        TRANSACTION_IDS_WITH_UUID_COMPARE_TRAPS.any { a ->
            TRANSACTION_IDS_WITH_UUID_COMPARE_TRAPS.any { b -> Integer.signum(UUID.fromString(a).compareTo(UUID.fromString(b))) != Integer.signum(a.compareTo(b)) }
        }

    data class EventSpec(
        val accountIndex: Int,
        val timestampOffsetMicros: Int,
        val transactionIdIndex: Int,
        val uppercaseTransactionId: Boolean,
        val accountIdOverride: String? = null,
    ) {
        val accountId: String get() = accountIdOverride ?: ACCOUNTS[accountIndex]

        fun withAccount(id: String): EventSpec = copy(accountIdOverride = id)

        val timestampMicros: Long get() = BASE_TIMESTAMP_MICROS + timestampOffsetMicros
        val transactionId: String get() = TRANSACTION_IDS_WITH_UUID_COMPARE_TRAPS[transactionIdIndex]

        val precedenceKey: Pair<Long, String> get() = timestampMicros to transactionId

        fun toEvent(): TransactionEvent {
            val content = derivedContentOf(this)
            return transactionEvent(
                transactionId = if (uppercaseTransactionId) transactionId.uppercase() else transactionId,
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

    data class ContentDerivedFromKey(
        val ownerId: String,
        val accountStatus: AccountStatus,
        val transactionStatus: TransactionStatus,
        val balanceAmount: String,
        val currency: String,
        val accountCreatedAtMicros: Long,
    )

    fun derivedContentOf(spec: EventSpec): ContentDerivedFromKey {
        val seed = Random("${spec.accountId}|${spec.timestampMicros}|${spec.transactionId}".hashCode().toLong()).nextLong()
        val random = Random(seed)
        return ContentDerivedFromKey(
            ownerId = UUID(seed, seed.rotateLeft(17) xor 0x5DEECE66DL).toString(),
            accountStatus = if (random.nextBoolean()) AccountStatus.ENABLED else AccountStatus.DISABLED,
            transactionStatus = if (random.nextBoolean()) TransactionStatus.APPROVED else TransactionStatus.DECLINED,
            balanceAmount = BigDecimal(BigInteger.valueOf(random.nextInt(1_000_000_000).toLong()), random.nextInt(4)).toPlainString(),
            currency = if (random.nextBoolean()) "BRL" else "USD",
            accountCreatedAtMicros = 1_500_000_000_000_000L + random.nextInt(1_000_000),
        )
    }

    fun winnerOf(specs: List<EventSpec>): EventSpec? =
        specs.maxWithOrNull(compareBy<EventSpec> { it.timestampMicros }.thenBy { it.transactionId })

    fun eventSpecs(accounts: Int): Arb<EventSpec> =
        Arb.bind(
            Arb.int(0 until accounts),
            Arb.int(0 until TIE_PRONE_TIMESTAMP_SPAN),
            Arb.element(TRANSACTION_IDS_WITH_UUID_COMPARE_TRAPS.indices.toList()),
            Arb.boolean(),
        ) { accountIndex, timestampOffsetMicros, transactionIdIndex, uppercase -> EventSpec(accountIndex, timestampOffsetMicros, transactionIdIndex, uppercase) }
}
