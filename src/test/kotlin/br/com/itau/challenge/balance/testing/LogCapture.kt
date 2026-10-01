package br.com.itau.challenge.balance.testing

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.slf4j.LoggerFactory

class LogCapture(
    private val loggerName: String,
    private val level: Level? = null,
) : BeforeEachCallback,
    AfterEachCallback {
    constructor(type: Class<*>, level: Level? = null) : this(type.name, level)

    private val appender = ListAppender<ILoggingEvent>()
    private lateinit var logger: Logger
    private var originalLevel: Level? = null

    val events: List<ILoggingEvent> get() = appender.list.toList()

    val messages: List<String> get() = events.map { it.formattedMessage }

    fun at(level: Level): List<ILoggingEvent> = events.filter { it.level == level }

    fun messageStartingWith(prefix: String): ILoggingEvent = events.single { it.formattedMessage.startsWith(prefix) }

    override fun beforeEach(context: ExtensionContext) {
        logger = LoggerFactory.getLogger(loggerName) as Logger
        originalLevel = logger.level
        appender.list.clear()
        appender.start()
        logger.addAppender(appender)
        level?.let { logger.level = it }
    }

    override fun afterEach(context: ExtensionContext) {
        logger.detachAppender(appender)
        appender.stop()
        logger.level = originalLevel
    }
}
