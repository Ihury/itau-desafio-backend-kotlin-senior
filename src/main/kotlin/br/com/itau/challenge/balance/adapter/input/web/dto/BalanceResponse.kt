package br.com.itau.challenge.balance.adapter.input.web.dto

import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import com.fasterxml.jackson.annotation.JsonProperty
import java.math.BigDecimal
import java.time.ZoneId
import java.time.format.DateTimeFormatter

data class BalanceResponse(
    val id: String,
    val owner: String,
    val balance: MoneyResponse,
    @get:JsonProperty("updated_at") val updatedAt: String,
) {
    data class MoneyResponse(
        val amount: BigDecimal,
        val currency: String,
    )

    companion object {
        fun from(
            snapshot: BalanceSnapshot,
            displayZone: ZoneId,
        ): BalanceResponse =
            BalanceResponse(
                id = snapshot.accountId.value,
                owner = snapshot.ownerId.value,
                balance = MoneyResponse(snapshot.balance.paddedToCurrencyScale(), snapshot.balance.currency.value),
                updatedAt = snapshot.precedence.timestamp.toInstant().atZone(displayZone).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
            )
    }
}
