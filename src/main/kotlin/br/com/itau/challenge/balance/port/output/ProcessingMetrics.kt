package br.com.itau.challenge.balance.port.output

/** Contadores de desfecho do processamento: exatamente um desfecho por evento aplicado (FR-031). */
interface ProcessingMetrics {
    /** O evento superou o snapshot vigente (`processed`). */
    fun applied()

    /** O snapshot vigente tinha precedencia maior (`obsolete`). */
    fun obsolete()

    /** Mesmo evento ja aplicado (`duplicate`); [conflicting] registra tambem a anomalia de conteudo divergente. */
    fun duplicate(conflicting: Boolean)
}
