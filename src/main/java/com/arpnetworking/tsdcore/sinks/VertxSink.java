/**
 * Copyright 2014 Brandon Arp
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.utility.DefaultHostNameResolver;
import com.arpnetworking.utility.HostNameResolver;
import com.google.common.collect.EvictingQueue;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.constraint.Range;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Context;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VertxFactory;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultContext;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Abstract publisher to send data to a server via Vertx <code>NetSocket</code>.
 *
 * This class is best described as 3 separate parts:
 * <ul>
 *     <li>The public interface</li>
 *     <li>The connect loop</li>
 *     <li>The send loop</li>
 * </ul>
 *
 * <p>
 *     The job of the public interface is to isolate the vertx event loop that sits at the
 *     heart of the sink.  The public interface, therefore provides the thread safety
 *     to the other two components.
 *
 *     Notably, the main way it interacts with the vertx event loop is by dispatching runnables
 *     to it.
 * </p>
 * <p>
 *     The connect loop runs on the vertx event loop and is tasked with maintaining the
 *     connection to the upstream server.  This is done by calling connectToServer.
 *     When an error is detected on the socket, the callback fires and again calls
 *     connectToServer.  If the connection fails, connectToServer is called in a vertx
 *     setTimer call, thus making it a loop.
 * </p>
 * <p>
 *     The send loop also runs on the vertx event loop and is tasked with sending the queued
 *     data to the connected socket.  If a connected socket does not exist, the loop will "sleep"
 *     by re-scheduling itself with the vertx setTimer call. The main function for this loop is
 *     consumeLoop.
 * </p>
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public abstract class VertxSink extends BaseSink {
    @Override
    public void close() {
        dispatch(
                event -> {
                    final NetSocket socket = _socket.getAndSet(null);
                    if (socket != null) {
                        socket.close();
                    }
                });
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("serverAddress", _serverAddress)
                .put("serverPort", _serverPort)
                .put("connecting", _connecting)
                .put("pendingDataSize", _pendingData.size())
                .build();
    }

    /**
     * Perform tasks when the connection is first established. This method is
     * invoked while holding a lock on the socket.
     *
     * @param socket The <code>NetSocket</code> instance that was connected.
     */
    protected void onConnect(final NetSocket socket) { }

    /**
     * Adds a {@link Buffer} of data to the pending data queue.
     *
     * @param data The data to add to the queue.
     */
    protected void enqueueData(final Buffer data) {
        dispatch(
                event -> {
                    if (_pendingData.remainingCapacity() == 0) {
                        LOGGER.warn()
                                .setMessage("Dropping data due to queue full")
                                .addData("sink", getName())
                                .log();
                    }
                    _pendingData.add(data);
                });
    }

    /**
     * Sends a <code>Buffer</code> of bytes to the socket if the client is connected.
     *
     * @param data the data to send
     */
    protected void sendRawData(final Buffer data) {
        dispatch(
                event -> {
                    final NetSocket socket = _socket.get();
                    try {
                        if (socket != null) {
                            socket.write(data);
                        } else {
                            LOGGER.warn()
                                    .setMessage("Could not write data to socket, socket is not connected")
                                    .addData("sink", getName())
                                    .log();
                        }

                        // CHECKSTYLE.OFF: IllegalCatch - Vertx might not log
                    } catch (final Exception e) {
                        // CHECKSTYLE.ON: IllegalCatch
                        if (socket != null) {
                            socket.close();
                        }
                        LOGGER.error()
                                .setMessage("Error writing data to socket")
                                .addData("sink", getName())
                                .setThrowable(e)
                                .log();
                        throw e;
                    }
                });
    }

    /**
     * Accessor for <code>Vertx</code> instance.
     *
     * @return The <code>Vertx</code> instance.
     */
    protected Vertx getVertx() {
        return _vertx;
    }

    private void dispatch(final Handler<Void> handler) {
        if (_context != null) {
            _context.runOnContext(handler);
        } else {
            _vertx.runOnContext(handler);
        }
    }

    /**
     * This function need only be called once, now in the constructor.
     */
    //TODO(barp): Move to a start/stop model for Sinks [MAI-257]
    private void connectToServer() {
        // Check if already connected
        if (_socket.get() != null) {
            return;
        }

        // Block if already connecting
        final boolean isConnecting = _connecting.getAndSet(true);
        if (isConnecting) {
            LOGGER.debug()
                    .setMessage("Already connecting, not attempting another connection at this time")
                    .addData("sink", getName())
                    .log();
            return;
        }

        // Don't try to connect too frequently
        final long currentTime = System.currentTimeMillis();
        if (currentTime - _lastConnectionAttempt < _currentReconnectWait) {
            LOGGER.debug()
                    .setMessage("Not attempting connection")
                    .addData("sink", getName())
                    .log();
            _connecting.set(false);
            return;
        }

        // Resolve the hostname
        final String resolvedHostname = _hostnameResolver.resolve(_serverAddress);

        // Attempt to connect
        LOGGER.info()
                .setMessage("Connecting to server")
                .addData("sink", getName())
                .addData("attempt", _connectionAttempt)
                .addData("address", resolvedHostname)
                .addData("port", _serverPort)
                .log();
        _lastConnectionAttempt = currentTime;
        _client.connect(
                _serverPort,
                resolvedHostname,
                new ConnectionHandler(resolvedHostname));
    }

    private Handler<Void> createSocketCloseHandler(final NetSocket socket) {
        return event -> {
            if (socket != null) {
                socket.close();
            }
            LOGGER.warn()
                    .setMessage("Server socket closed; forcing reconnect attempt")
                    .addData("sink", getName())
                    .log();
            _socket.set(null);
            _lastConnectionAttempt = 0;
            connectToServer();
        };
    }

    private Handler<Throwable> createSocketExceptionHandler() {
        return event -> LOGGER.warn()
                .setMessage("Server socket exception")
                .addData("sink", getName())
                .setThrowable(event)
                .log();
    }

    private void consumeLoop() {
        long flushedBytes = 0;
        try {
            boolean done = false;
            NetSocket socket = _socket.get();
            if (!_pendingData.isEmpty()) {
                LOGGER.debug()
                        .setMessage("Pending data")
                        .addData("sink", getName())
                        .addData("size", _pendingData.size())
                        .log();
            }
            while (socket != null && !done) {
                if (_pendingData.size() > 0 && flushedBytes < MAX_FLUSH_BYTES) {
                    final Buffer buffer = _pendingData.poll();
                    flushedBytes += flushBuffer(buffer, socket);
                } else {
                    done = true;
                }
                socket = _socket.get();
            }
            if (socket == null
                    && (_lastNotConnectedNotify == null
                    || _lastNotConnectedNotify.plus(Duration.standardSeconds(30)).isBeforeNow())) {
                LOGGER.debug()
                        .setMessage(
                                "Not connected to server. Data will be flushed when reconnected. "
                                + "Suppressing this message for 30 seconds.")
                        .addData("sink", getName())
                        .log();
                _lastNotConnectedNotify = DateTime.now();
            }
            // CHECKSTYLE.OFF: IllegalCatch - Vertx might not log
        } catch (final Exception e) {
            // CHECKSTYLE.ON: IllegalCatch
            LOGGER.error()
                    .setMessage("Error in consume loop")
                    .addData("sink", getName())
                    .setThrowable(e)
                    .log();
            throw e;
        } finally {
            if (flushedBytes > 0) {
                dispatch(event -> consumeLoop());
            } else {
                getVertx().setTimer(NO_DATA_CONSUME_LOOP_INTERVAL, event -> consumeLoop());
            }
        }
    }

    private int flushBuffer(final Buffer buffer, final NetSocket socket) {
        // Write the serialized data
        try {
            final int bufferLength = buffer.length();
            // TODO(vkoskela): Add conditional logging [AINT-552]
            //LOGGER.trace(String.format("Writing buffer to socket; length=%s buffer=%s", bufferLength, buffer.toString("utf-8")));
            LOGGER.debug()
                    .setMessage("Writing buffer to socket")
                    .addData("sink", getName())
                    .addData("length", bufferLength)
                    .log();
            socket.write(buffer);
            return bufferLength;
        // CHECKSTYLE.OFF: IllegalCatch - Vertx might not log
        } catch (final Exception e) {
        // CHECKSTYLE.ON: IllegalCatch
            LOGGER.error()
                    .setMessage("Error writing AggregatedData data to socket")
                    .addData("sink", getName())
                    .addData("buffer", buffer)
                    .setThrowable(e)
                    .log();
            throw e;
        }
    }

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    protected VertxSink(final Builder<?, ?> builder) {
        super(builder);
        _serverAddress = builder._serverAddress;
        _hostnameResolver = builder._hostnameResolver;
        _serverPort = builder._serverPort;
        _vertx = VertxFactory.newVertx();
        //Calling this just so the context gets created
        if (_vertx instanceof DefaultVertx) {
            final DefaultVertx vertx = (DefaultVertx) _vertx;
            final DefaultContext context = vertx.getOrCreateContext();
            vertx.setContext(context);
            _context = context;
        } else {
            _context = null;
            LOGGER.warn()
                    .setMessage("Vertx instance not a DefaultVertx as expected. Threading may be incorrect.")
                    .addData("sink", getName())
                    .log();
        }

        _client = _vertx.createNetClient()
                .setReconnectAttempts(0)
                .setConnectTimeout(5000)
                .setTCPNoDelay(true)
                .setTCPKeepAlive(true);
        _socket = new AtomicReference<>();
        _pendingData = EvictingQueue.create(builder._maxQueueSize);
        _exponentialBackoffBase = builder._exponentialBackoffBase;

        connectToServer();
        consumeLoop();
    }

    private final String _serverAddress;
    private final HostNameResolver _hostnameResolver;
    private final int _serverPort;
    private final Vertx _vertx;
    private final NetClient _client;
    private final Context _context;
    private final AtomicReference<NetSocket> _socket;
    private final EvictingQueue<Buffer> _pendingData;
    private final AtomicBoolean _connecting = new AtomicBoolean(false);
    private DateTime _lastNotConnectedNotify = null;
    private volatile long _lastConnectionAttempt = 0;
    private volatile int _connectionAttempt = 1;
    private final int _exponentialBackoffBase;

    private int _currentReconnectWait = 3000;

    private static final Logger LOGGER = LoggerFactory.getLogger(VertxSink.class);
    private static final long MAX_FLUSH_BYTES = 2 ^ 20; // 1 Mebibyte
    private static final int NO_DATA_CONSUME_LOOP_INTERVAL = 100;

    private class ConnectionHandler implements AsyncResultHandler<NetSocket> {

        @Override
        public void handle(final AsyncResult<NetSocket> event) {
            if (event.succeeded()) {
                LOGGER.info()
                        .setMessage("Connected to server")
                        .addData("sink", getName())
                        .addData("address", _hostname)
                        .addData("port", _serverPort)
                        .addData("attempt", _connectionAttempt)
                        .log();
                final NetSocket socket = event.result();
                socket.exceptionHandler(createSocketExceptionHandler());
                socket.endHandler(createSocketCloseHandler(socket));
                _connectionAttempt = 1;

                onConnect(socket);

                _connecting.set(false);
                _socket.set(socket);
            } else if (event.failed()) {
                LOGGER.warn()
                        .setMessage("Error connecting to server")
                        .addData("sink", getName())
                        .addData("address", _hostname)
                        .addData("port", _serverPort)
                        .setThrowable(event.cause())
                        .log();
                _connectionAttempt++;
                //Calculate the next reconnect delay.  Exponential backoff formula.

                _currentReconnectWait = (((int) (Math.random()  //randomize
                        * Math.pow(1.3, Math.min(_connectionAttempt, 20)))) //1.3^x where x = min(attempt, 20)
                        +  1) //make sure we don't wait 0
                        *  _exponentialBackoffBase; //the milliseconds base
                LOGGER.info()
                        .setMessage("Waiting")
                        .addData("sink", getName())
                        .addData("currentReconnectWait", _currentReconnectWait)
                        .log();
                getVertx().setTimer(_currentReconnectWait, handler -> connectToServer());
                final NetSocket socket = event.result();
                if (socket != null) {
                    socket.close();
                }
                _connecting.set(false);
                _socket.set(null);
            }
        }

        ConnectionHandler(final String hostname) {
            _hostname = hostname;
        }

        private final String _hostname;
    }

    /**
     * Implementation of base builder pattern for <code>VertxSink</code>.
     *
     * @param <B> type of the builder
     * @param <S> type of the object to be built
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public abstract static class Builder<B extends BaseSink.Builder<B, S>, S extends Sink> extends BaseSink.Builder<B, S> {

        /**
         * The server host name. Cannot be null or empty.
         *
         * @param value The aggregation server host name.
         * @return This instance of <code>Builder</code>.
         */
        public B setServerAddress(final String value) {
            _serverAddress = value;
            return self();
        }

        /**
         * The host name resolver. Optional. Defaults to {@link DefaultHostNameResolver}. Cannot be null.
         *
         * @param value The host name resolver.
         * @return This instance of <code>Builder</code>.
         */
        public B setHostNameResolver(final HostNameResolver value) {
            _hostnameResolver = value;
            return self();
        }

        /**
         * The server port. Cannot be null; must be between 1 and 65535.
         *
         * @param value The server port.
         * @return This instance of <code>Builder</code>.
         */
        public B setServerPort(final Integer value) {
            _serverPort = value;
            return self();
        }

        /**
         * The maximum queue size. Cannot be null. Default is 10000.
         *
         * @param value The maximum queue size.
         * @return This instance of <code>Builder</code>.
         */
        public B setMaxQueueSize(final Integer value) {
            _maxQueueSize = value;
            return self();
        }

        /**
         * Protected constructor for subclasses.
         *
         * @param targetConstructor The constructor for the concrete type to be created by this builder.
         */
        protected Builder(final Function<B, S> targetConstructor) {
            super(targetConstructor);
        }

        @NotNull
        @NotEmpty
        private String _serverAddress;
        @NotNull
        private HostNameResolver _hostnameResolver = DEFAULT_HOSTNAME_RESOLVER;
        @NotNull
        @Range(min = 1, max = 65535)
        private Integer _serverPort;
        @NotNull
        @Min(value = 0)
        private Integer _maxQueueSize = 10000;
        @NotNull
        private Integer _exponentialBackoffBase = 500;

        private static final HostNameResolver DEFAULT_HOSTNAME_RESOLVER = new DefaultHostNameResolver();
    }
}
