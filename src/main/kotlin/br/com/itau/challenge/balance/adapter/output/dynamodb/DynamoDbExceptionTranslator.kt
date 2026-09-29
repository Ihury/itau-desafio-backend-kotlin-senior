package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException
import software.amazon.awssdk.core.exception.ApiCallTimeoutException
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException
import software.amazon.awssdk.services.dynamodb.model.ThrottlingException

/**
 * Traduz falhas do SDK para as excecoes de dominio. Toda falha do SDK e tratada como indisponibilidade transitoria
 * (jamais "nao encontrada" nem isolamento de mensagem valida: FR-017/FR-025); a unica excecao e a
 * `ValidationException` na ESCRITA, que e uma rejeicao do armazenamento. A excecao original vai em `cause`.
 * Excecoes que nao sao do SDK nao sao traduzidas (`null`).
 */
internal object DynamoDbExceptionTranslator {
    private val throttlingCodes = setOf("ThrottlingException", "ProvisionedThroughputExceededException", "RequestLimitExceeded")

    fun forRead(failure: Throwable): BalanceStoreUnavailableException? =
        if (failure is SdkException) BalanceStoreUnavailableException(causeOf(failure), failure) else null

    fun forWrite(failure: Throwable): RuntimeException? =
        when {
            failure !is SdkException -> null
            isValidationError(failure) -> BalanceStoreRejectedException(failure)
            else -> BalanceStoreUnavailableException(causeOf(failure), failure)
        }

    private fun causeOf(failure: SdkException): StoreFailureCause =
        when {
            failure is ProvisionedThroughputExceededException ||
                failure is RequestLimitExceededException ||
                failure is ThrottlingException ||
                errorCode(failure) in throttlingCodes -> StoreFailureCause.THROTTLED
            failure is ApiCallTimeoutException || failure is ApiCallAttemptTimeoutException -> StoreFailureCause.TIMEOUT
            else -> StoreFailureCause.UNAVAILABLE
        }

    private fun isValidationError(failure: SdkException): Boolean = errorCode(failure) == "ValidationException"

    /** Codigo do erro sem o prefixo de namespace (`com.amazon.coral.validate#ValidationException`). */
    private fun errorCode(failure: SdkException): String? =
        (failure as? AwsServiceException)?.awsErrorDetails()?.errorCode()?.substringAfterLast('#')
}
