/**
 * Copyright 2014 Groupon.com
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

import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for the <code>AggregationServerSink</code> class.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class AggregationServerSinkTest {

    @Before
    public void setUp() throws IOException {
        _port = NEXT_PORT.getAndIncrement();
        _serverChannel = ServerSocketChannel.open();
        _serverChannel.bind(new InetSocketAddress(_port)).configureBlocking(false);
    }

    @After
    public void tearDown() throws IOException {
        if (_serverChannel != null) {
            _serverChannel.close();
        }
    }

    @Test
    public void connectTest() throws IOException, InterruptedException {
        AggregationServerSink sink = null;
        try {

            sink = new AggregationServerSink.Builder()
                    .setName("foo-name")
                    .setServerAddress("localhost")
                    .setServerPort(_port)
                    .build();

            final PeriodicData data = TestBeanFactory.createPeriodicData();
            sink.recordAggregateData(data);

            final SocketChannel connectedSocket = listenForConnection(_serverChannel, Duration.standardSeconds(5));
            Assert.assertNotNull(connectedSocket);
        } finally {
            if (sink != null) {
                sink.close();
            }
        }
    }

    @Test
    public void reconnectTest() throws IOException, InterruptedException {
        AggregationServerSink sink = null;
        try {
            sink = new AggregationServerSink.Builder()
                    .setName("foo-name")
                    .setServerAddress("localhost")
                    .setServerPort(_port)
                    .build();

            final PeriodicData data = TestBeanFactory.createPeriodicData();
            sink.recordAggregateData(data);

            SocketChannel connectedSocket = listenForConnection(_serverChannel, Duration.standardSeconds(5));
            Thread.sleep(1000);
            connectedSocket.close();
            _serverChannel.close();


            spammyWait(3000, sink);
            _serverChannel = ServerSocketChannel.open();
            _serverChannel.bind(new InetSocketAddress(_port)).configureBlocking(false);

            //Wait for reconnect
            connectedSocket = listenForConnection(_serverChannel, Duration.standardSeconds(10));
            Assert.assertNotNull(connectedSocket);

        } finally {
            if (sink != null) {
                sink.close();
            }
        }
    }

    private void spammyWait(final int wait, final AggregationServerSink sink) throws InterruptedException {
        final DateTime start = DateTime.now();

        while (DateTime.now().isBefore(start.plusMillis(wait))) {
            sink.recordAggregateData(TestBeanFactory.createPeriodicData());
            Thread.sleep(500);
        }
    }

    private SocketChannel listenForConnection(final ServerSocketChannel serverChannel,
                                              final Duration timeout) throws IOException, InterruptedException {
        SocketChannel connectedSocket = null;
        boolean connected = false;
        final DateTime start = DateTime.now();
        while (!connected) {
            if (DateTime.now().isAfter(start.plus(timeout))) {
                throw new RuntimeException("Connection not established within timeout");
            }
            final SocketChannel socketChannel = serverChannel.accept();
            if (socketChannel != null) {
                connected = true;
                connectedSocket = socketChannel;
            } else {
                Thread.sleep(20);
            }
        }
        return connectedSocket;
    }

    private ServerSocketChannel _serverChannel;
    private int _port;

    private static final AtomicInteger NEXT_PORT = new AtomicInteger(17065);
}
