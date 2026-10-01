package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.adapter.input.MdcKeys
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

class InvalidAccountIdException : RuntimeException("invalid account id")

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
        MDC.put(MdcKeys.ACCOUNT_ID, id.value)
        val snapshot = getBalanceUseCase.getBalance(id)
        return ResponseEntity
            .ok()
            .cacheControl(CacheControl.noStore())
            .body(BalanceResponse.from(snapshot, displayZone))
    }
}
