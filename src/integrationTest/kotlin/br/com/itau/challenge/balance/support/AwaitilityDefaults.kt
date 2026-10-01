package br.com.itau.challenge.balance.support

import org.awaitility.Awaitility
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import java.time.Duration

class AwaitilityDefaults : BeforeAllCallback {
    override fun beforeAll(context: ExtensionContext) {
        Awaitility.setDefaultTimeout(DEFAULT_TIMEOUT)
        Awaitility.setDefaultPollInterval(DEFAULT_POLL_INTERVAL)
    }

    private companion object {
        val DEFAULT_TIMEOUT: Duration = Duration.ofSeconds(30)
        val DEFAULT_POLL_INTERVAL: Duration = Duration.ofMillis(100)
    }
}
