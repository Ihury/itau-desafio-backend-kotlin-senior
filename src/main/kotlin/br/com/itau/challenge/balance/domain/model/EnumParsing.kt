package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidEventException

internal inline fun <reified E : Enum<E>> parseEnum(raw: String): E =
    enumValues<E>().firstOrNull { it.name == raw } ?: throw InvalidEventException(RejectionReason.UNKNOWN_DOMAIN_VALUE)
