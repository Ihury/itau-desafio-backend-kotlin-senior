package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.testing.DISPLAY_ZONE
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import java.time.ZoneId

@TestConfiguration
class WebTestBeans {
    @Bean
    fun displayZone(): ZoneId = DISPLAY_ZONE
}
