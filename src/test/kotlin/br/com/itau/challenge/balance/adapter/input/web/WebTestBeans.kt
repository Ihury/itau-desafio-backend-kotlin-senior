package br.com.itau.challenge.balance.adapter.input.web

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import java.time.ZoneId

/** Beans do composition root que os testes de fatia web (`@WebMvcTest`) nao carregam. */
@TestConfiguration
class WebTestBeans {
    @Bean
    fun displayZone(): ZoneId = ZoneId.of("America/Sao_Paulo")
}
