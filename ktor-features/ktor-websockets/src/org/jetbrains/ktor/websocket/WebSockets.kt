package org.jetbrains.ktor.websocket

import kotlinx.coroutines.experimental.*
import org.jetbrains.ktor.application.*
import org.jetbrains.ktor.util.*
import java.util.concurrent.*

class WebSockets {
    val hostPool: ExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors())
    val appPool: ExecutorService = Executors.newCachedThreadPool()

    val hostDispatcher: CoroutineDispatcher = hostPool.asCoroutineDispatcher()
    val appDispatcher: CoroutineDispatcher = appPool.asCoroutineDispatcher()

    private fun stopping() {
        hostPool.shutdown()
        appPool.shutdown()
    }

    private fun stopped() {
        hostPool.shutdownNow()
        appPool.shutdownNow()
    }

    class WebbSocketOptions {
    }

    companion object : ApplicationFeature<Application, WebbSocketOptions, WebSockets> {
        override val key = AttributeKey<WebSockets>("WebSockets")

        override fun install(pipeline: Application, configure: WebbSocketOptions.() -> Unit): WebSockets {
            return WebbSocketOptions().also(configure).let { options ->
                val webSockets = WebSockets()

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