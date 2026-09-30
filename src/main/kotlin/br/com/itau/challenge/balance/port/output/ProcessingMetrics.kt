package br.com.itau.challenge.balance.port.output

import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause

/** Contadores de desfecho do processamento: exatamente um desfecho por evento aplicado. */
interface ProcessingMetrics {
    /** O evento superou o snapshot vigente. */
    fun processed()

    /** O evento tem precedencia menor que a do snapshot vigente. */
    fun obsolete()

    /** Mesmo evento ja aplicado (`duplicate`); [conflicting] registra tambem a anomalia de conteudo divergente. */
    fun duplicate(conflicting: Boolean)

    /** A mensagem foi isolada no DLT com o [reason] do catalogo. So e chamado depois que o DLT confirma a publicacao. */
    fun rejected(reason: RejectionReason)

    /** A publicacao no DLT falhou: a mensagem NAO foi confirmada e sera reentregue (alertar). Nao e um desfecho. */
    fun dltPublishFailed()

    /** O consumer pausou a leitura por falha transitoria do armazenamento; a mensagem valida segue no broker. Nao e um desfecho. */
    fun backpressure(cause: StoreFailureCause)
}
