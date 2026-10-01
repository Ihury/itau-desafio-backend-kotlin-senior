package br.com.itau.challenge.balance.support

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import br.com.itau.challenge.balance.port.output.BalanceSnapshotWriter
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class DefectiveWriter(
    private val delegate: BalanceSnapshotWriter,
) : BalanceSnapshotWriter {
    val poisoned: MutableSet<String> = ConcurrentHashMap.newKeySet()
    val attempts = ConcurrentHashMap<String, AtomicInteger>()
    val lastThread = ConcurrentHashMap<String, String>()

    override fun applyIfNewer(snapshot: BalanceSnapshot): ApplyResult {
        val account = snapshot.accountId.value
        if (account !in poisoned) return delegate.applyIfNewer(snapshot)
        lastThread[account] = Thread.currentThread().name
        attempts.computeIfAbsent(account) { AtomicInteger() }.incrementAndGet()
        throw IllegalStateException("defeito simulado")
    }
}

@TestConfiguration
class DefectiveWriterConfig {
    @Bean
    @Primary
    fun defectiveWriter(
        @Qualifier("balanceSnapshotWriter") delegate: BalanceSnapshotWriter,
    ): DefectiveWriter = DefectiveWriter(delegate)
}
