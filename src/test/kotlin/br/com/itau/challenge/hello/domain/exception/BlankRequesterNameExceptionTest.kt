package br.com.itau.challenge.hello.domain.exception

import kotlin.test.Test
import kotlin.test.assertEquals

class BlankRequesterNameExceptionTest {

    @Test
    fun `should carry a descriptive message`() {
        val exception = BlankRequesterNameException()

        assertEquals("Requester name must not be blank", exception.message)
    }
}
