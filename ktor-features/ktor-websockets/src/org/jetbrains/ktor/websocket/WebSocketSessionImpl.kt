package org.jetbrains.ktor.websocket

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.*
import org.jetbrains.ktor.application.*
import org.jetbrains.ktor.cio.*
import java.io.*
import java.time.*
import java.util.concurrent.atomic.*
import kotlin.coroutines.experimental.*

internal class WebSocketSessionImpl(call: ApplicationCall,
                                    val readChannel: ReadChannel,
                                    val writeChannel: WriteChannel,
                                    val channel: Closeable,
                                    val pool: ByteBufferPool = NoPool,
                                    val webSockets: WebSockets,
                                    val hostContext: CoroutineContext,
                                    val userContext: CoroutineContext,
                                    val handle: suspend WebSocketSession.() -> Unit
                                    ) : WebSocketSession(call) {

    private val messageHandler = actor<Frame>(hostContext, capacity = 8, start = CoroutineStart.LAZY) {
        consumeEach { msg ->
            when (msg) {
                is Frame.Close -> {
                    closeSequence.send(CloseFrameEvent.Received(msg))
                }
                is Frame.Pong -> {
                    try {
                        pingPongJob.get()?.send(msg)
                    } catch (ignore: ClosedSendChannelException) {
                    }
                }
                is Frame.Ping -> {
                    ponger.send(msg)
                }
                else -> {
                }
            }

            run(webSockets.appDispatcher) {
                frameHandler(msg)
            }
        }
    }

    private val writer = WebSocketWriter(writeChannel, hostContext, pool)
    private val reader = WebSocketReader(readChannel, { maxFrameSize }, messageHandler)

    private val closeSequence = closeSequence(hostContext, writer, timeout) { reason ->
        launch(hostContext) {
            terminateConnection(reason)
        }
    }

    private val pingPongJob = AtomicReference<ActorJob<Frame.Pong>?>()
    private val ponger = ponger(hostContext, this, pool)

    override var masking: Boolean
        get() = writer.masking
        set(value) {
            writer.masking = value
        }

    init {
        masking = false
    }

    fun start() {
        val rj = reader.start(hostContext, pool)
        val wj = writer.start(hostContext, pool)

        rj.invokeOnCompletion { t ->
            launch(hostContext) {
                val reason = when (t) {
                    null -> null
                    is WebSocketReader.FrameTooBigException -> {
                        CloseReason(CloseReason.Codes.TOO_BIG, t.message)
                    }
                    else -> {
                        CloseReason(CloseReason.Codes.UNEXPECTED_CONDITION, t.javaClass.name)
                    }
                }

                try {
                    if (reason != null) {
                        close(reason)
                    }
                } catch (ignore: ClosedSendChannelException) {
                }
            }

            messageHandler.close(t)
        }

        wj.invokeOnCompletion { t ->
            messageHandler.close(t)

            launch(hostContext) {
                try {
                    closeSequence.send(CloseFrameEvent.Died)
                } catch (ignore: ClosedSendChannelException) {
                }
            }
        }
    }

    override var pingInterval: Duration? = null
        set(value) {
            field = value
            if (value == null) {
                pingPongJob.getAndSet(null)?.cancel()
            } else {
                val j = pinger(hostContext, this, value, timeout, pool, closeSequence)
                pingPongJob.getAndSet(j)?.cancel()
                j.start()
            }
        }

    suspend override fun flush() {
        writer.flush()
    }

    override suspend fun send(frame: Frame) {
        if (frame is Frame.Close) {
            closeSequence.send(CloseFrameEvent.ToSend(frame))
        } else {
            writer.send(frame)
        }
    }

    suspend override fun awaitClose() {
        closeSequence.join()
    }

    override fun terminate() {
        super.terminate()

        writer.close()
        messageHandler.close()
        pingPongJob.getAndSet(null)?.cancel()
        ponger.close()
        closeSequence.cancel()

        try {
            readChannel.close()
        } catch (t: Throwable) {
            application.log.debug("Failed to close read channel")
        }

        try {
            writeChannel.close()
        } catch (t: Throwable) {
            application.log.debug("Failed to close write channel")
        }

        try {
            channel.close()
        } catch (t: Throwable) {
            application.log.debug("Failed to close write channel")
        }

        runBlocking(userContext) {
            closeHandler(null)
        }
    }

    suspend fun terminateConnection(reason: CloseReason?) {
        try {
            run(webSockets.appDispatcher) {
                closeHandler(reason)
            }
        } finally {
            terminate()
        }
    }
}