package br.com.itau.challenge.balance.domain.exception

import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame

class DomainExceptionsTest {
    private val accountId = AccountId.parse("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")

    @Test
    fun `AccountNotFoundException carries only the account id`() {
        val error = AccountNotFoundException(accountId)

        assertEquals(accountId, error.accountId)
        assertEquals("account not found: ${accountId.value}", error.message)
    }

    @Test
    fun `AccountDisabledException carries only the account id`() {
        val error = AccountDisabledException(accountId)

        assertEquals(accountId, error.accountId)
        assertEquals("account disabled: ${accountId.value}", error.message)
    }

    @Test
    fun `BalanceStoreUnavailableException carries the failure cause and the original exception`() {
        val original = IllegalStateException("sdk")
        val error = BalanceStoreUnavailableException(StoreFailureCause.THROTTLED, original)

        assertEquals(StoreFailureCause.THROTTLED, error.failureCause)
        assertSame(original, error.cause)
        assertIs<RuntimeException>(error)
        assertEquals("balance store unavailable: THROTTLED", error.message)
    }

    @Test
    fun `BalanceStoreUnavailableException original exception is optional`() {
        val error = BalanceStoreUnavailableException(StoreFailureCause.TIMEOUT)

        assertNull(error.cause)
        assertEquals(StoreFailureCause.TIMEOUT, error.failureCause)
    }

    @Test
    fun `store failure causes are throttled, unavailable and timeout`() {
        assertEquals(setOf("THROTTLED", "UNAVAILABLE", "TIMEOUT"), StoreFailureCause.entries.map { it.name }.toSet())
    }

    @Test
    fun `BalanceStoreRejectedException is not the transient exception`() {
        val original = IllegalArgumentException("validation")
        val error: Throwable = BalanceStoreRejectedException(original)

        assertSame(original, error.cause)
        assertFalse(error is BalanceStoreUnavailableException)
        assertNull(BalanceStoreRejectedException().cause)
        assertEquals("balance store rejected the write", error.message)
    }
}
