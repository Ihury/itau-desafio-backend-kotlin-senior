package br.com.itau.challenge.balance.port.input

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.TransactionEvent

interface ProcessTransactionEventUseCase {
    fun process(event: TransactionEvent): ApplyResult
}
