/**
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

import akka.actor.ActorRef;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.headers.RawHeader;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.http.RequestReply;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.test.TestBeanFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
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
public class CollectdHttpSourceV1Test extends ActorSourceTest {

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        _source = new CollectdHttpSourceV1.Builder()
                .setActorName("collectd")
                .setName("collectd_source")
                .setParser(_parser)
                .build();
        _source.attach(_observer);
    }

    @Test
    public void testParsesEntity() throws ExecutionException, ParsingException {
        final byte[] entity = "not a json document".getBytes(Charsets.UTF_8);
        Mockito.when(_parser.parse(Mockito.any())).thenThrow(new ParsingException("test exception"));
        dispatchRequest(HttpRequest.create().withEntity(entity));
        Mockito.verify(_parser).parse(entity);
    }

    @Test
    public void test200OnEmptyData() throws ExecutionException, ParsingException {
        Mockito.when(_parser.parse(Mockito.any())).thenReturn(Collections.emptyList());
        final HttpResponse response = dispatchRequest();
        Assert.assertEquals(200, response.status().intValue());
    }

    @Test
    public void test400OnBadData() throws ParsingException, ExecutionException {
        Mockito.when(_parser.parse(Mockito.any())).thenThrow(new ParsingException("test exception"));
        final HttpResponse response = dispatchRequest();
        Assert.assertEquals(400, response.status().intValue());
    }

    @Test
    public void test400OnMissingTags() throws ParsingException, ExecutionException, InterruptedException {
        final DefaultRecord.Builder builder = new DefaultRecord.Builder()
                .setHost("fake-host")
                .setTime(DateTime.now())
                .setId("id")
                .setMetrics(Collections.emptyMap());
        Mockito.when(_parser.parse(Mockito.any())).thenReturn(Collections.singletonList(builder));
        final HttpResponse response = dispatchRequest();
        final String errorString = response.entity()
                .toStrict(500, null)
                .toCompletableFuture()
                .get()
                .getData()
                .utf8String();
        Assert.assertEquals(400, response.status().intValue());
        Assert.assertTrue(errorString.contains("service cannot be null"));
        Assert.assertTrue(errorString.contains("cluster cannot be null"));
    }

    @Test
    public void testParsesTagHeaders() throws ParsingException, ExecutionException, InterruptedException {
        final DefaultRecord.Builder builder = TestBeanFactory.createRecordBuilder()
                .setService(null)
                .setCluster(null);

        final String service = "my-service";
        final String cluster = "my-cluster";
        final HttpRequest request = HttpRequest.create()
                .addHeader(RawHeader.create("X-TAG-SERVICE", service))
                .addHeader(RawHeader.create("X-TAG-CLUSTER", cluster));
        Mockito.when(_parser.parse(Mockito.any())).thenReturn(Collections.singletonList(builder));
        final HttpResponse response = dispatchRequest(request);
        Assert.assertEquals(200, response.status().intValue());

        final Record record = builder.build();
        Assert.assertEquals(cluster, record.getCluster());
        Assert.assertEquals(service, record.getService());
    }

    @Test
    public void testSendsRecords() throws ParsingException, ExecutionException, InterruptedException {
        final DefaultRecord.Builder builder1 = TestBeanFactory.createRecordBuilder();
        final DefaultRecord.Builder builder2 = TestBeanFactory.createRecordBuilder();
        final DefaultRecord.Builder builder3 = TestBeanFactory.createRecordBuilder();
        final ArrayList<DefaultRecord.Builder> builders = Lists.newArrayList(builder1, builder2, builder3);

        Mockito.when(_parser.parse(Mockito.any())).thenReturn(builders);
        final HttpResponse response = dispatchRequest();
        Assert.assertEquals(200, response.status().intValue());
        final ArgumentCaptor<Record> builderCaptor = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_observer, Mockito.times(3)).notify(Mockito.any(), builderCaptor.capture());

        final List<Record> constructed = builderCaptor.getAllValues();
        Assert.assertEquals(builder1.build(), constructed.get(0));
        Assert.assertEquals(builder2.build(), constructed.get(1));
        Assert.assertEquals(builder3.build(), constructed.get(2));
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
            throw Throwables.propagate(e);
        }
    }

    @Mock
    private Observer _observer;
    @Mock
    private Parser<List<DefaultRecord.Builder>> _parser;
    private CollectdHttpSourceV1 _source;
}
