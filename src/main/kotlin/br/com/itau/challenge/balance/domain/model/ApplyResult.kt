package br.com.itau.challenge.balance.domain.model

/** Desfecho da aplicacao de um evento ao armazenamento (classificacao exclusiva). */
sealed interface ApplyResult {
    /** O evento superou o snapshot vigente (ou o criou). */
    data object Applied : ApplyResult

    /** O snapshot vigente tem precedencia maior: sem efeito, nao e erro. */
    data object Obsolete : ApplyResult

    /** Mesmo evento ja aplicado; [conflicting] indica conteudo divergente (anomalia da origem). */
    data class Duplicate(
        val conflicting: Boolean,
    ) : ApplyResult
}
