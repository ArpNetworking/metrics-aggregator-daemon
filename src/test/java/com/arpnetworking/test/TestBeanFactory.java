/*
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
package com.arpnetworking.test;

import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.Unit;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Creates reasonable random instances of common data types for testing. This is
 * strongly preferred over mocking data type classes as mocking should be
 * reserved for defining behavior and not data.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class TestBeanFactory {

    /**
     * Create a builder for pseudo-random <code>Record</code>.
     *
     * @return New builder for pseudo-random <code>Record</code>.
     */
    public static DefaultRecord.Builder createRecordBuilder() {
        return new DefaultRecord.Builder()
                .setMetrics(
                        ImmutableMap.of(
                                "foo/bar",
                                createMetric()))
                .setTime(ZonedDateTime.now())
                .setId(UUID.randomUUID().toString())
                .setDimensions(
                        ImmutableMap.of(
                                Key.HOST_DIMENSION_KEY, "MyHost",
                                Key.SERVICE_DIMENSION_KEY, "MyService",
                                Key.CLUSTER_DIMENSION_KEY, "MyCluster"));
    }

    /**
     * Create a new reasonable pseudo-random <code>Record</code>.
     *
     * @return New reasonable pseudo-random <code>Record</code>.
     */
    public static Record createRecord() {
        return createRecordBuilder().build();
    }

    /**
     * Create a builder for pseudo-random <code>Metric</code>.
     *
     * @return New builder for pseudo-random <code>Metric</code>.
     */
    public static DefaultMetric.Builder createMetricBuilder() {
        return new DefaultMetric.Builder()
                .setType(MetricType.COUNTER)
                .setValues(ImmutableList.of(new DefaultQuantity.Builder()
                        .setValue(1.23d)
                        .build()));
    }

    /**
     * Create a new reasonable pseudo-random <code>Metric</code>.
     *
     * @return New reasonable pseudo-random <code>Metric</code>.
     */
    public static Metric createMetric() {
        return createMetricBuilder().build();
    }

    /**
     * Create a builder for pseudo-random <code>AggregatedData</code>.
     *
     * @return New builder for pseudo-random <code>AggregatedData</code>.
     */
    public static AggregatedData.Builder createAggregatedDataBuilder() {
        return new AggregatedData.Builder()
                .setStatistic(MEAN_STATISTIC)
                .setValue(createSample())
                .setIsSpecified(true)
                .setPopulationSize((long) (Math.random() * 100));
    }

    /**
     * Create a new reasonable pseudo-random <code>AggregatedData</code>.
     *
     * @return New reasonable pseudo-random <code>AggregatedData</code>.
     */
    public static AggregatedData createAggregatedData() {
        return createAggregatedDataBuilder().build();
    }

    /**
     * Create a builder for pseudo-random <code>PeriodicData</code>.
     *
     * @return New builder for pseudo-random <code>PeriodicData</code>.
     */
    public static PeriodicData.Builder createPeriodicDataBuilder() {
        return new PeriodicData.Builder()
                .setDimensions(
                        new DefaultKey(
                                ImmutableMap.of(
                                        Key.HOST_DIMENSION_KEY, "host-" + UUID.randomUUID(),
                                        Key.SERVICE_DIMENSION_KEY, "service-" + UUID.randomUUID(),
                                        Key.CLUSTER_DIMENSION_KEY, "cluster-" + UUID.randomUUID())))
                .setData(ImmutableMultimap.of("metric-" + UUID.randomUUID(), createAggregatedData()))
                .setPeriod(Duration.ofMinutes(5))
                .setStart(ZonedDateTime.now());
    }

    /**
     * Create a new reasonable pseudo-random <code>PeriodicData</code>.
     *
     * @return New reasonable pseudo-random <code>PeriodicData</code>.
     */
    public static PeriodicData createPeriodicData() {
        return createPeriodicDataBuilder().build();
    }

    /**
     * Create a builder for reasonable pseudo-random <code>Quantity</code>.
     *
     * @return New builder for reasonable pseudo-random <code>Quantity</code>.
     */
    public static DefaultQuantity.Builder createSampleBuilder() {
        return new DefaultQuantity.Builder().setValue(Math.random()).setUnit(Unit.BIT);
    }

    /**
     * Create a new reasonable pseudo-random <code>Quantity</code>.
     *
     * @return New reasonable pseudo-random <code>Quantity</code>.
     */
    public static Quantity createSample() {
        return new DefaultQuantity.Builder().setValue(Math.random()).setUnit(Unit.BIT).build();
    }

    /**
     * Create a <code>List</code> of <code>Quantity</code> instances in
     * <code>Unit.MILLISECOND</code> from a <code>List</code> of <code>Double</code>
     * values.
     *
     * @param values The values.
     * @return <code>List</code> of <code>Quantity</code> instances.
     */
    public static List<Quantity> createSamples(final List<Double> values) {
        return FluentIterable.from(Lists.newArrayList(values)).transform(CREATE_SAMPLE).toList();
    }

    private static final Function<Double, Quantity> CREATE_SAMPLE =
            input -> new DefaultQuantity.Builder().setValue(input).setUnit(Unit.MILLISECOND).build();

    private TestBeanFactory() {}

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEAN_STATISTIC = STATISTIC_FACTORY.getStatistic("mean");
}
