package br.com.itau.challenge.balance.port.output

import br.com.itau.challenge.balance.domain.model.RejectionReason
import br.com.itau.challenge.balance.domain.model.StoreFailureCause

/** Contadores de desfecho do processamento: exatamente um desfecho por evento aplicado (FR-031). */
interface ProcessingMetrics {
    /** O evento superou o snapshot vigente (`processed`). */
    fun processed()

    /** O snapshot vigente tinha precedencia maior (`obsolete`). */
    fun obsolete()

    /** Mesmo evento ja aplicado (`duplicate`); [conflicting] registra tambem a anomalia de conteudo divergente. */
    fun duplicate(conflicting: Boolean)

    /**
     * A mensagem foi isolada no DLT com o [reason] do catalogo (`rejected`). So e chamado depois que o DLT confirma a
     * publicacao, nunca em tentativas.
     */
    fun rejected(reason: RejectionReason)

    /** A publicacao no DLT falhou: a mensagem NAO foi confirmada e sera reentregue (alertar). Nao e um desfecho. */
    fun dltPublishFailed()

    /**
     * O consumer pausou a leitura por uma falha transitoria do armazenamento (indisponibilidade da ingestao); [cause] e a
     * classificacao da falha. Nao e um desfecho: a mensagem valida segue no broker e sera reentregue.
     */
    fun backpressure(cause: StoreFailureCause)
}
