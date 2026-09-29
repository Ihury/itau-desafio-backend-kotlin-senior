package br.com.itau.challenge.hello.adapter.input.web

import br.com.itau.challenge.hello.adapter.input.web.dto.GreetingResponse
import br.com.itau.challenge.hello.port.input.GetGreetingUseCase
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
class GreetingController(
    private val getGreetingUseCase: GetGreetingUseCase,
) {

    @GetMapping("/hello", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun hello(@RequestParam name: String): GreetingResponse {
        val greeting = getGreetingUseCase.getGreeting(name)
        return GreetingResponse(message = greeting.message)
    }
}
