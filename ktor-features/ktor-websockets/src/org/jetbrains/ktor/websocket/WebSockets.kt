package org.jetbrains.ktor.websocket

import kotlinx.coroutines.experimental.*
import org.jetbrains.ktor.application.*
import org.jetbrains.ktor.util.*
import java.time.*
import java.util.concurrent.*

class WebSockets(
        val pingInterval: Duration?,
        val timeout: Duration,
        val maxFrameSize: Long,
        val masking: Boolean
) {
    @Deprecated("")
    val hostPool: ExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors())

    @Deprecated("")
    val appPool: ExecutorService = Executors.newCachedThreadPool()

    @Deprecated("")
    val hostDispatcher: CoroutineDispatcher = hostPool.asCoroutineDispatcher()

    @Deprecated("")
    val appDispatcher: CoroutineDispatcher = appPool.asCoroutineDispatcher()

    private fun stopping() {
        hostPool.shutdown()
        appPool.shutdown()
    }

    private fun stopped() {
        hostPool.shutdownNow()
        appPool.shutdownNow()
    }

    class WebSocketOptions {
        var pingPeriod: Duration? = null
        var timeout: Duration = Duration.ofSeconds(15)
        var maxFrameSize = Long.MAX_VALUE
        var masking: Boolean = false
    }

    companion object : ApplicationFeature<Application, WebSocketOptions, WebSockets> {
        override val key = AttributeKey<WebSockets>("WebSockets")

        override fun install(pipeline: Application, configure: WebSocketOptions.() -> Unit): WebSockets {
            return WebSocketOptions().also(configure).let { options ->
                val webSockets = WebSockets(options.pingPeriod, options.timeout, options.maxFrameSize, options.masking)

                pipeline.environment.monitor.applicationStopping += {
                    webSockets.stopping()
                }

                pipeline.environment.monitor.applicationStopped += {
                    webSockets.stopped()
                }

                webSockets
            }
        }
    }
}