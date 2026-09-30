package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.exception.BalanceStoreRejectedException
import br.com.itau.challenge.balance.domain.exception.BalanceStoreUnavailableException
import br.com.itau.challenge.balance.domain.model.StoreFailureCause
import br.com.itau.challenge.balance.domain.model.StoreFailureDetails
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException
import software.amazon.awssdk.core.exception.ApiCallTimeoutException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.core.exception.SdkServiceException
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ThrottlingException

/**
 * Toda falha do SDK e indisponibilidade transitoria (jamais "nao encontrada" nem isolamento de mensagem valida); a unica
 * excecao e a `ValidationException` na ESCRITA, que e uma rejeicao do armazenamento. A excecao original vai em `cause`.
 * Excecoes que nao sao do SDK nao sao traduzidas (`null`).
 *
 * Tabela inexistente, acesso negado e problemas de credencial ([StoreFailureCause.MISCONFIGURED]) tambem sao transitorios (a
 * correcao e operacional e a mensagem nunca vai ao DLT); so o diagnostico muda.
 */
internal object DynamoDbExceptionTranslator {
    private val throttlingCodes = setOf("ThrottlingException", "ProvisionedThroughputExceededException", "RequestLimitExceeded")

    /** Codigos do servico que indicam configuracao/credencial (`ExpiredToken*` e tratado a parte, por prefixo). */
    private val misconfigurationCodes =
        setOf(
            "ResourceNotFoundException",
            "AccessDeniedException",
            "UnrecognizedClientException",
            "InvalidSignatureException",
            "MissingAuthenticationToken",
            "MissingAuthenticationTokenException",
        )

    /** Prefixo das mensagens do SDK quando a cadeia de provedores nao resolve nenhuma credencial (`SdkClientException`). */
    private const val CREDENTIALS_MESSAGE_PREFIX = "Unable to load credentials"

    private const val MAX_CAUSE_DEPTH = 5
    private const val VALIDATION_EXCEPTION_CODE = "ValidationException"
    private val safeErrorCode = Regex("[A-Za-z0-9_.#:-]{1,100}")

    fun translateReadFailure(failure: Throwable): BalanceStoreUnavailableException? = if (failure is SdkException) toUnavailable(failure) else null

    fun translateWriteFailure(failure: Throwable): RuntimeException? =
        when {
            failure !is SdkException -> null
            isValidationError(failure) -> BalanceStoreRejectedException(failure)
            else -> toUnavailable(failure)
        }

    private fun toUnavailable(failure: SdkException) = BalanceStoreUnavailableException(causeOf(failure), failure, detailsOf(failure))

    private fun detailsOf(failure: SdkException): StoreFailureDetails =
        StoreFailureDetails(
            exceptionClass = failure.javaClass.name,
            // vem do servidor: so um token curto e seguro vai ao log (sem quebra de linha nem texto livre)
            errorCode = errorCode(failure)?.takeIf { safeErrorCode.matches(it) },
            statusCode = (failure as? SdkServiceException)?.statusCode()?.takeIf { it > 0 },
        )

    private fun causeOf(failure: SdkException): StoreFailureCause =
        when {
            failure is ProvisionedThroughputExceededException ||
                failure is RequestLimitExceededException ||
                failure is ThrottlingException ||
                errorCode(failure) in throttlingCodes -> StoreFailureCause.THROTTLED
            isMisconfiguration(failure) -> StoreFailureCause.MISCONFIGURED
            failure is ApiCallTimeoutException || failure is ApiCallAttemptTimeoutException -> StoreFailureCause.TIMEOUT
            else -> StoreFailureCause.UNAVAILABLE
        }

    private fun isMisconfiguration(failure: SdkException): Boolean {
        val code = errorCode(failure)
        return failure is ResourceNotFoundException ||
            code in misconfigurationCodes ||
            code?.startsWith("ExpiredToken") == true ||
            isCredentialsFailure(failure)
    }

    /** `SdkClientException` (ou uma causa dela) cuja mensagem e a da cadeia de provedores; so o inicio e comparado. */
    private fun isCredentialsFailure(failure: Throwable): Boolean =
        failure is SdkClientException &&
            generateSequence<Throwable>(failure) { it.cause }
                .take(MAX_CAUSE_DEPTH)
                .any { it.message?.startsWith(CREDENTIALS_MESSAGE_PREFIX) == true }

    private fun isValidationError(failure: SdkException): Boolean = errorCode(failure) == VALIDATION_EXCEPTION_CODE

    /** Codigo do erro sem o prefixo de namespace (`com.amazon.coral.validate#ValidationException`). */
    private fun errorCode(failure: SdkException): String? =
        (failure as? AwsServiceException)?.awsErrorDetails()?.errorCode()?.substringAfterLast('#')
}
