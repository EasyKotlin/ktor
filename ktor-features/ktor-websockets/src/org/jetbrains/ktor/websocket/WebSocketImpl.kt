package org.jetbrains.ktor.websocket

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.*
import org.jetbrains.ktor.application.*
import org.jetbrains.ktor.cio.*
import org.jetbrains.ktor.pipeline.*
import java.io.*
import java.time.*
import java.util.concurrent.atomic.*

internal class WebSocketImpl(call: ApplicationCall,
                             val readChannel: ReadChannel,
                             val writeChannel: WriteChannel,
                             val channel: Closeable,
                             val pool: ByteBufferPool = NoPool) : WebSocket(call) {

    private val messageHandler = actor<Frame>(application.executor.asCoroutineDispatcher(), capacity = 8, start = CoroutineStart.LAZY) {
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

            frameHandler(msg)
        }
    }

    private val writer = WebSocketWriter(writeChannel)
    private val reader = WebSocketReader(readChannel, { maxFrameSize }, messageHandler)

    private val closeSequence = closeSequence(application.executor.asCoroutineDispatcher(), writer, timeout) { reason ->
        launch(application.executor.asCoroutineDispatcher()) {
            terminateConnection(reason)
        }
    }

    private val pingPongJob = AtomicReference<ActorJob<Frame.Pong>?>()
    private val ponger = ponger(application.executor.asCoroutineDispatcher(), this, pool)

    override var masking: Boolean
        get() = writer.masking
        set(value) {
            writer.masking = value
        }

    init {
        masking = false
    }

    fun start() {
        launchAsync(application.executor) {
            val ticket = pool.allocate(DEFAULT_BUFFER_SIZE)

            try {
                reader.readLoop(ticket.buffer)
            } catch (tooBig: WebSocketReader.FrameTooBigException) {
                errorHandler(tooBig)
                close(CloseReason(CloseReason.Codes.TOO_BIG, tooBig.message))
            } catch (t: Throwable) {
                errorHandler(t)
                close(CloseReason(CloseReason.Codes.UNEXPECTED_CONDITION, t.javaClass.name))
            } finally {
                pool.release(ticket)
                closeSequence.start()
            }
        }

        launchAsync(application.executor) {
            val ticket = pool.allocate(DEFAULT_BUFFER_SIZE)

            try {
                writer.writeLoop(ticket.buffer)
            } catch (t: Throwable) {
                errorHandler(t)
            } finally {
                pool.release(ticket)
                closeSequence.start()
            }
        }
    }

    override var pingInterval: Duration? = null
        set(value) {
            field = value
            if (value == null) {
                pingPongJob.getAndSet(null)?.cancel()
            } else {
                val j = pinger(application.executor.asCoroutineDispatcher(), this, value, timeout, pool, closeSequence)
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

        runBlocking {
            closeHandler(null)
        }
    }

    suspend fun terminateConnection(reason: CloseReason?) {
        try {
            closeHandler(reason)
        } finally {
            terminate()
        }
    }
}