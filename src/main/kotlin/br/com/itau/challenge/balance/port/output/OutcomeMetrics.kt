package br.com.itau.challenge.balance.port.output

import br.com.itau.challenge.balance.domain.model.ApplyResult

interface OutcomeMetrics {
    fun record(result: ApplyResult)
}
