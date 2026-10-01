package br.com.itau.challenge.balance.port.output

import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause

interface ConsumerFailureMetrics {
    fun rejected(reason: RejectionReason)

    fun dltPublishFailed()

    fun backpressure(cause: StoreFailureCause)
}
