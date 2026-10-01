package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.ApplyResult
import br.com.itau.challenge.balance.domain.model.BalanceSnapshot
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

internal object ConflictClassifier {
    fun classify(
        candidate: BalanceSnapshot,
        currentItem: Map<String, AttributeValue>,
    ): ApplyResult {
        val currentPrecedence = BalanceItemMapper.precedenceOf(currentItem)
        return when {
            currentPrecedence == candidate.precedence -> ApplyResult.Duplicate(conflicting = hasDivergentContent(candidate, currentItem))
            currentPrecedence > candidate.precedence -> ApplyResult.Obsolete
            else -> contradiction()
        }
    }

    private fun hasDivergentContent(
        candidate: BalanceSnapshot,
        currentItem: Map<String, AttributeValue>,
    ): Boolean =
        BalanceItemMapper.ownerOf(currentItem) != candidate.ownerId ||
            BalanceItemMapper.statusOf(currentItem) != candidate.status ||
            BalanceItemMapper.balanceOf(currentItem) != candidate.balance

    private fun contradiction(): Nothing = throw IllegalStateException("current balance item contradicts the failed condition")
}
