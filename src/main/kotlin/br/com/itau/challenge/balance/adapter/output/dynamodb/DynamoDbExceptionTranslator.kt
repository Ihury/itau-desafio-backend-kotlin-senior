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

internal object DynamoDbExceptionTranslator {
    private val throttlingCodes = setOf("ThrottlingException", "ProvisionedThroughputExceededException", "RequestLimitExceeded")

    private val misconfigurationCodes =
        setOf(
            "ResourceNotFoundException",
            "AccessDeniedException",
            "UnrecognizedClientException",
            "InvalidSignatureException",
            "MissingAuthenticationToken",
            "MissingAuthenticationTokenException",
        )

    private const val SDK_NO_CREDENTIALS_MESSAGE_PREFIX = "Unable to load credentials"
    private const val EXPIRED_TOKEN_PREFIX = "ExpiredToken"
    private const val MAX_CAUSE_DEPTH = 5
    private const val VALIDATION_EXCEPTION_CODE = "ValidationException"
    private const val NAMESPACE_SEPARATOR = '#'
    private val safeErrorCodePattern = Regex("[A-Za-z0-9_.#:-]{1,100}")

    fun translateReadFailure(failure: Throwable): Throwable = if (failure is SdkException) toUnavailable(failure) else failure

    fun translateWriteFailure(failure: Throwable): Throwable =
        when {
            failure !is SdkException -> failure
            isValidationError(failure) -> BalanceStoreRejectedException(failure)
            else -> toUnavailable(failure)
        }

    private fun toUnavailable(failure: SdkException) = BalanceStoreUnavailableException(causeOf(failure), failure, detailsOf(failure))

    private fun detailsOf(failure: SdkException): StoreFailureDetails =
        StoreFailureDetails(
            exceptionClass = failure.javaClass.name,
            errorCode = unqualifiedErrorCode(failure)?.takeIf { safeErrorCodePattern.matches(it) },
            statusCode = (failure as? SdkServiceException)?.statusCode()?.takeIf { it > 0 },
        )

    private fun causeOf(failure: SdkException): StoreFailureCause =
        when {
            failure is ProvisionedThroughputExceededException ||
                failure is RequestLimitExceededException ||
                failure is ThrottlingException ||
                unqualifiedErrorCode(failure) in throttlingCodes -> StoreFailureCause.THROTTLED
            isMisconfiguration(failure) -> StoreFailureCause.MISCONFIGURED
            failure is ApiCallTimeoutException || failure is ApiCallAttemptTimeoutException -> StoreFailureCause.TIMEOUT
            else -> StoreFailureCause.UNAVAILABLE
        }

    private fun isMisconfiguration(failure: SdkException): Boolean {
        val code = unqualifiedErrorCode(failure)
        return failure is ResourceNotFoundException ||
            code in misconfigurationCodes ||
            code?.startsWith(EXPIRED_TOKEN_PREFIX) == true ||
            isCredentialsFailure(failure)
    }

    private fun isCredentialsFailure(failure: Throwable): Boolean =
        failure is SdkClientException &&
            generateSequence<Throwable>(failure) { it.cause }
                .take(MAX_CAUSE_DEPTH)
                .any { it.message?.startsWith(SDK_NO_CREDENTIALS_MESSAGE_PREFIX) == true }

    private fun isValidationError(failure: SdkException): Boolean = unqualifiedErrorCode(failure) == VALIDATION_EXCEPTION_CODE

    private fun unqualifiedErrorCode(failure: SdkException): String? =
        (failure as? AwsServiceException)?.awsErrorDetails()?.errorCode()?.substringAfterLast(NAMESPACE_SEPARATOR)
}
