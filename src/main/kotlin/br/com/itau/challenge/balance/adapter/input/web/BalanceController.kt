package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.adapter.input.web.dto.BalanceResponse
import br.com.itau.challenge.balance.domain.exception.InvalidEventException
import br.com.itau.challenge.balance.domain.model.AccountId
import br.com.itau.challenge.balance.port.input.GetBalanceUseCase
import org.slf4j.MDC
import org.springframework.http.CacheControl
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import java.time.ZoneId

/** O `accountId` da URL nao e um UUID canonico; a consulta ao armazenamento nao chega a ser feita (FR-024). */
class InvalidAccountIdException : RuntimeException("invalid account id")

/**
 * `GET /balances/{accountId}`. O identificador chega como `String` e e validado por `AccountId.parse` (regex estrita:
 * `UUID.fromString` aceitaria `1-1-1-1-1`) ANTES de tocar o caso de uso. So a falha do parse vira 400: uma
 * `InvalidEventException` que escapasse do caso de uso e defeito interno (500).
 */
@RestController
class BalanceController(
    private val getBalanceUseCase: GetBalanceUseCase,
    private val displayZone: ZoneId,
) {
    @GetMapping("/balances/{accountId}")
    fun getBalance(
        @PathVariable accountId: String,
    ): ResponseEntity<BalanceResponse> {
        val id =
            try {
                AccountId.parse(accountId)
            } catch (_: InvalidEventException) {
                throw InvalidAccountIdException()
            }
        MDC.put(CorrelationIdFilter.MDC_ACCOUNT_ID, id.value)
        val snapshot = getBalanceUseCase.getBalance(id)
        return ResponseEntity
            .ok()
            .cacheControl(CacheControl.noStore())
            .body(BalanceResponse.from(snapshot, displayZone))
    }
}
