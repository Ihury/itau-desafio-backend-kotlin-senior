package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.exception.AccountDisabledException
import br.com.itau.challenge.balance.domain.exception.AccountNotFoundException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.TransactionStatus
import br.com.itau.challenge.balance.testing.InMemoryBalanceStore
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.DEFAULT_ACCOUNT_ID
import br.com.itau.challenge.balance.testing.TransactionEventFixtures.transactionEvent
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

class GetBalanceServiceTest {
    private val store = InMemoryBalanceStore()
    private val service = GetBalanceService(store)
    private val accountId = AccountId.parse(DEFAULT_ACCOUNT_ID)

    private fun snapshotOf(
        status: AccountStatus = AccountStatus.ENABLED,
        timestampMicros: Long = 1751749453433000L,
        transactionStatus: TransactionStatus = TransactionStatus.APPROVED,
        balanceAmount: String = "183.12",
    ): BalanceSnapshot =
        BalanceSnapshot.from(
            transactionEvent(
                accountStatus = status,
                timestampMicros = timestampMicros,
                transactionStatus = transactionStatus,
                balanceAmount = balanceAmount,
            ),
        )

    @Test
    fun `returns the current snapshot when the account is enabled`() {
        val snapshot = snapshotOf()
        store.seed(snapshot)

        assertSame(snapshot, service.getBalance(accountId))
    }

    @Test
    fun `missing account raises not found and never a zero balance`() {
        val failure = assertFailsWith<AccountNotFoundException> { service.getBalance(accountId) }

        assertEquals(accountId, failure.accountId)
    }

    @Test
    fun `disabled snapshot raises account disabled carrying no balance data`() {
        store.seed(snapshotOf(status = AccountStatus.DISABLED, balanceAmount = "999.99"))

        val failure = assertFailsWith<AccountDisabledException> { service.getBalance(accountId) }

        assertEquals(accountId, failure.accountId)
        assertTrue("999" !in failure.message.orEmpty(), "a mensagem nao pode carregar saldo")
    }

    @Test
    fun `a newer enabled snapshot replacing a disabled one makes the query succeed`() {
        store.seed(snapshotOf(status = AccountStatus.DISABLED, timestampMicros = 1751749453433000L))
        assertFailsWith<AccountDisabledException> { service.getBalance(accountId) }

        val newer = snapshotOf(status = AccountStatus.ENABLED, timestampMicros = 1751749454433000L, balanceAmount = "70.00")
        store.seed(newer)

        assertSame(newer, service.getBalance(accountId))
    }

    @Test
    fun `store unavailability propagates intact and never becomes not found`() {
        val failure = BalanceStoreUnavailableException(StoreFailureCause.TIMEOUT)
        store.failReadsWith(failure)

        val raised = assertFailsWith<BalanceStoreUnavailableException> { service.getBalance(accountId) }

        assertSame(failure, raised)
    }

    @Test
    fun `a snapshot that came from a declined event is served normally`() {
        val snapshot = snapshotOf(transactionStatus = TransactionStatus.DECLINED, balanceAmount = "0.10")
        store.seed(snapshot)

        assertSame(snapshot, service.getBalance(accountId))
    }
}
