package br.com.itau.challenge.hello.domain.model

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class GreetingTest {

    @Test
    fun `should expose the message it was created with`() {
        val greeting = Greeting(message = "Hello, Ada!")

        assertEquals("Hello, Ada!", greeting.message)
    }

    @Test
    fun `should be equal when messages are equal`() {
        assertEquals(Greeting("Hello, Ada!"), Greeting("Hello, Ada!"))
    }

    @Test
    fun `should not be equal when messages differ`() {
        assertNotEquals(Greeting("Hello, Ada!"), Greeting("Hi, Ada!"))
    }
}
