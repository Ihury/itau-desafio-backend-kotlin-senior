package br.com.itau.challenge.hello.domain.exception

import kotlin.test.Test
import kotlin.test.assertEquals

class InvalidGreetingTemplateExceptionTest {

    @Test
    fun `should carry the message it was created with`() {
        val exception = InvalidGreetingTemplateException("Greeting template must not be blank")

        assertEquals("Greeting template must not be blank", exception.message)
    }
}
