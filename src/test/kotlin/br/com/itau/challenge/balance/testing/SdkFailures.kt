package br.com.itau.challenge.balance.testing

import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException

fun serviceError(
    status: Int,
    code: String,
): DynamoDbException =
    DynamoDbException
        .builder()
        .statusCode(status)
        .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).errorMessage("detalhe interno").build())
        .message("detalhe interno")
        .build() as DynamoDbException

fun credentialsFailure(): SdkClientException =
    SdkClientException
        .builder()
        .message("Unable to load credentials from any of the providers in the chain AwsCredentialsProviderChain: [...]")
        .build()
