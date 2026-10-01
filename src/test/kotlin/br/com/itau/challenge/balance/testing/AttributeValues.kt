package br.com.itau.challenge.balance.testing

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

fun stringAttr(value: String): AttributeValue = AttributeValue.builder().s(value).build()

fun numberAttr(value: String): AttributeValue = AttributeValue.builder().n(value).build()
