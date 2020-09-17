package com.arpnetworking.metrics.mad.sources;

import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.metrics.common.sources.Source;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.sources.ratelimiter.AcceptListStore;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.ZonedDateTime;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class DistinctOriginLimitingSourceTest {
    private Source _mockSource;
    private AcceptListStore _mockStore;
    private Observer _mockObserver;
    private DistinctOriginLimitingSource.Builder _sourceBuilder;

    @Before
    public void setUp() {
        _mockSource = Mockito.mock(Source.class);
        _mockStore = Mockito.mock(AcceptListStore.class);
        Mockito.when(_mockStore.updateAndReturnCurrent(any())).thenReturn(ImmutableSet.of());
        _mockObserver = Mockito.mock(Observer.class);
        _sourceBuilder = new DistinctOriginLimitingSource.Builder()
                .setName("DistinctOriginLimitingSource")
                .setSource(_mockSource)
                .setStore(_mockStore);
    }

    @Test
    public void testAttach() {
        _sourceBuilder.build();
        Mockito.verify(_mockSource).attach(Mockito.any(Observer.class));
    }

    @Test
    public void testStart() {
        _sourceBuilder.build().start();
        Mockito.verify(_mockSource).start();
        Mockito.verify(_mockStore).updateAndReturnCurrent(Mockito.any());
    }

    @Test
    public void testStop() {
        _sourceBuilder.build().stop();
        Mockito.verify(_mockSource).stop();
    }

    @Test
    public void testToString() {
        final String asString = _sourceBuilder.build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void testUsesStoreList() {
        final String metricHash = Hashing.sha256().hashBytes("foobar".getBytes()).toString();
        reset(_mockStore);
        when(_mockStore.updateAndReturnCurrent(any()))
                .thenReturn(ImmutableSet.<String>builder().add(metricHash).build());
        Source source = _sourceBuilder.build();
        source.attach(_mockObserver);
        source.start();

        notify(_mockSource, new DefaultRecord.Builder()
                .setMetrics(ImmutableMap.of("foobar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build()))
                .setId(UUID.randomUUID().toString())
                .setTime(ZonedDateTime.now())
                .build()
        );

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(source), argument.capture());
        final Record actualRecord = argument.getValue();
        Assert.assertNotNull(actualRecord.getMetrics().get("foobar"));
    }

    @Test
    public void testDoesNothingIfNotARecord() {
        final String metricHash = Hashing.sha256().hashBytes("foobar".getBytes()).toString();
        reset(_mockStore);
        when(_mockStore.updateAndReturnCurrent(any()))
                .thenReturn(ImmutableSet.<String>builder().add(metricHash).build());
        Source source = _sourceBuilder.build();
        source.attach(_mockObserver);
        source.start();

        notify(_mockSource, new Object());

        Mockito.verifyNoMoreInteractions(_mockObserver);
    }

    @Test
    public void testFullyBlocksRecordIfNoneAccepted() {
        final String metricHash = Hashing.sha256().hashBytes("foobar".getBytes()).toString();
        reset(_mockStore);
        when(_mockStore.updateAndReturnCurrent(any()))
                .thenReturn(ImmutableSet.<String>builder().add(metricHash).build());
        Source source = _sourceBuilder.build();
        source.attach(_mockObserver);
        source.start();

        notify(_mockSource, new DefaultRecord.Builder()
                .setMetrics(ImmutableMap.of("bar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build()))
                .setId(UUID.randomUUID().toString())
                .setTime(ZonedDateTime.now())
                .build()
        );

        Mockito.verifyNoMoreInteractions(_mockObserver);
    }

    @Test
    public void testPartiallyBlocksRecordIfSomeAccepted() {
        final String metricHash = Hashing.sha256().hashBytes("foobar".getBytes()).toString();
        reset(_mockStore);
        when(_mockStore.updateAndReturnCurrent(any()))
                .thenReturn(ImmutableSet.<String>builder().add(metricHash).build());
        Source source = _sourceBuilder.build();
        source.attach(_mockObserver);
        source.start();

        notify(_mockSource, new DefaultRecord.Builder()
                .setMetrics(ImmutableMap.of("bar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build(),
                        "foobar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build()))
                .setId(UUID.randomUUID().toString())
                .setTime(ZonedDateTime.now())
                .build()
        );

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(source), argument.capture());
        final Record actualRecord = argument.getValue();
        Assert.assertEquals(1, actualRecord.getMetrics().size());
        Assert.assertNotNull(actualRecord.getMetrics().get("foobar"));
    }

    @Test
    public void testAcceptedAfterThresholdReached() {
        final String metricHash = Hashing.sha256().hashBytes("foobar".getBytes()).toString();
        reset(_mockStore);
        when(_mockStore.updateAndReturnCurrent(any()))
                .thenReturn(ImmutableSet.<String>builder().add(metricHash).build());
        Source source = _sourceBuilder.setThreshold(1).build();
        source.attach(_mockObserver);
        source.start();

        notify(_mockSource, new DefaultRecord.Builder()
                .setMetrics(ImmutableMap.of("bar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build(),
                        "foobar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build()))
                .setId(UUID.randomUUID().toString())
                .setTime(ZonedDateTime.now())
                .build()
        );

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(source), argument.capture());
        Record actualRecord = argument.getValue();
        Assert.assertEquals(1, actualRecord.getMetrics().size());
        Assert.assertNotNull(actualRecord.getMetrics().get("foobar"));
        reset(_mockObserver);

        notify(_mockSource, new DefaultRecord.Builder()
                .setAnnotations(ImmutableMap.of("origin", "1234"))
                .setMetrics(ImmutableMap.of("bar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build(),
                        "foobar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build()))
                .setId(UUID.randomUUID().toString())
                .setTime(ZonedDateTime.now())
                .build()
        );

        Mockito.verify(_mockObserver).notify(Mockito.same(source), argument.capture());
        actualRecord = argument.getValue();
        Assert.assertEquals(2, actualRecord.getMetrics().size());
        Assert.assertNotNull(actualRecord.getMetrics().get("foobar"));
        Assert.assertNotNull(actualRecord.getMetrics().get("bar"));
    }

    @Test
    public void testObserverRecordsAllAcceptedMetrics() {
        final DistinctOriginLimitingSource mockLimitingSource = mock(DistinctOriginLimitingSource.class);
        final DistinctOriginLimitingSource.DistinctOriginLimitingObserver observer =
                new DistinctOriginLimitingSource.DistinctOriginLimitingObserver(mockLimitingSource, Hashing.sha256(), 1);
        final String metricHash = Hashing.sha256().hashBytes("foobar".getBytes()).toString();
        observer.updateAcceptList(ImmutableSet.of(metricHash));

        observer.notify(mockLimitingSource, new DefaultRecord.Builder()
                .setMetrics(ImmutableMap.of("bar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build(),
                        "foobar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build()))
                .setId(UUID.randomUUID().toString())
                .setTime(ZonedDateTime.now())
                .build()
        );

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(mockLimitingSource).notify(argument.capture());
        Record actualRecord = argument.getValue();
        Assert.assertEquals(1, actualRecord.getMetrics().size());
        Assert.assertNotNull(actualRecord.getMetrics().get("foobar"));
        reset(mockLimitingSource);

        observer.notify(mockLimitingSource, new DefaultRecord.Builder()
                .setAnnotations(ImmutableMap.of("origin", "1234"))
                .setMetrics(ImmutableMap.of("bar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build(),
                        "foobar",
                        new DefaultMetric.Builder().setType(MetricType.COUNTER).build()))
                .setId(UUID.randomUUID().toString())
                .setTime(ZonedDateTime.now())
                .build()
        );

        Mockito.verify(mockLimitingSource).notify(argument.capture());
        actualRecord = argument.getValue();
        Assert.assertEquals(2, actualRecord.getMetrics().size());
        Assert.assertNotNull(actualRecord.getMetrics().get("foobar"));
        Assert.assertNotNull(actualRecord.getMetrics().get("bar"));

        final ImmutableSet<String> accepted = observer.getAcceptList();
        Assert.assertEquals(2, accepted.size());
        Assert.assertTrue(accepted.contains(metricHash));
        Assert.assertTrue(accepted.contains(Hashing.sha256().hashBytes("bar".getBytes()).toString()));
    }


    private static void notify(final Observable observable, final Object event) {
        final ArgumentCaptor<Observer> argument = ArgumentCaptor.forClass(Observer.class);
        Mockito.verify(observable).attach(argument.capture());
        for (final Observer observer : argument.getAllValues()) {
            observer.notify(observable, event);
        }
    }
}
