/*
 * Copyright 2016 Smartsheet
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
package com.arpnetworking.metrics.common.sources;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.http.javadsl.model.HttpRequest;
import org.apache.pekko.http.javadsl.model.HttpResponse;
import org.apache.pekko.http.javadsl.model.headers.RawHeader;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.http.RequestReply;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.test.TestBeanFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for the CollectdHttpSourceV1.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public final class CollectdHttpSourceV1Test extends BaseActorSourceTest {

    @Before
    public void setUp() {
        super.setUp();
        _mocks = MockitoAnnotations.openMocks(this);
        _source = new CollectdHttpSourceV1.Builder()
                .setActorName("collectd")
                .setName("collectd_source")
                .setParser(_parser)
                .setPeriodicMetrics(_periodicMetrics)
                .build();
        _source.attach(_observer);
    }

    @After
    public void after() throws Exception {
        _mocks.close();
    }

    @Test
    public void testParsesEntity() throws ParsingException {
        final byte[] entity = "not a json document".getBytes(Charsets.UTF_8);
        Mockito.when(_parser.parse(Mockito.any())).thenThrow(new ParsingException("test exception", new byte[0]));
        dispatchRequest(HttpRequest.create().withEntity(entity).addHeader(RawHeader.create("x-tag-foo", "bar")));
        final ArgumentCaptor<com.arpnetworking.metrics.mad.model.HttpRequest> captor =
                ArgumentCaptor.forClass(com.arpnetworking.metrics.mad.model.HttpRequest.class);
        Mockito.verify(_parser).parse(captor.capture());
        Assert.assertArrayEquals(entity, captor.getValue().getBody().toArray());
        Assert.assertEquals("bar", captor.getValue().getHeaders().get("x-tag-foo").toArray()[0]);
    }

    @Test
    public void test200OnEmptyData() throws ExecutionException, ParsingException {
        Mockito.when(_parser.parse(Mockito.any())).thenReturn(Collections.emptyList());
        final HttpResponse response = dispatchRequest();
        Assert.assertEquals(200, response.status().intValue());
    }

    @Test
    public void test400OnBadData() throws ParsingException, ExecutionException {
        Mockito.when(_parser.parse(Mockito.any())).thenThrow(new ParsingException("test exception", new byte[0]));
        final HttpResponse response = dispatchRequest();
        Assert.assertEquals(400, response.status().intValue());
    }

    @Test
    public void testSendsRecords() throws ParsingException, ExecutionException, InterruptedException {
        final Record record1 = TestBeanFactory.createRecord();
        final Record record2 = TestBeanFactory.createRecord();
        final Record record3 = TestBeanFactory.createRecord();
        final ArrayList<Record> builders = Lists.newArrayList(record1, record2, record3);

        Mockito.when(_parser.parse(Mockito.any())).thenReturn(builders);
        final HttpResponse response = dispatchRequest();
        Assert.assertEquals(200, response.status().intValue());
        final ArgumentCaptor<Record> builderCaptor = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_observer, Mockito.times(3)).notify(Mockito.any(), builderCaptor.capture());

        final List<Record> constructed = builderCaptor.getAllValues();
        Assert.assertEquals(record1, constructed.get(0));
        Assert.assertEquals(record2, constructed.get(1));
        Assert.assertEquals(record3, constructed.get(2));
    }

    private HttpResponse dispatchRequest() throws ExecutionException {
        return dispatchRequest(HttpRequest.create());
    }

    private HttpResponse dispatchRequest(final HttpRequest request) {
        final ActorRef ref = getSystem().actorOf(CollectdHttpSourceV1.Actor.props(_source));
        final CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        final RequestReply message = new RequestReply(request, future);

        ref.tell(message, ActorRef.noSender());

        try {
            return future.get(10000, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Mock
    private Observer _observer;
    @Mock
    private Parser<List<Record>, com.arpnetworking.metrics.mad.model.HttpRequest> _parser;
    @Mock
    private PeriodicMetrics _periodicMetrics;

    private CollectdHttpSourceV1 _source;
    private AutoCloseable _mocks;
}
