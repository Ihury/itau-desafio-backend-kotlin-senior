package br.com.itau.challenge.hello.port.input

import br.com.itau.challenge.hello.domain.model.Greeting

fun interface GetGreetingUseCase {
    fun getGreeting(requesterName: String): Greeting
}
