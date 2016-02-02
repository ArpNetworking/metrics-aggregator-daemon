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
package com.arpnetworking.test;

import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.Condition;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.UUID;

/**
 * Creates reasonable random instances of common data types for testing. This is
 * strongly preferred over mocking data type classes as mocking should be
 * reserved for defining behavior and not data.
 *
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public final class TestBeanFactory {

    /**
     * Create a builder for pseudo-random <code>Condition</code>.
     *
     * @return New builder for pseudo-random <code>Condition</code>.
     */
    public static Condition.Builder createConditionBuilder() {
        return new Condition.Builder()
                .setFQDSN(createFQDSN())
                .setName("condition-" + UUID.randomUUID())
                .setExtensions(ImmutableMap.of("severity", "severity-" + UUID.randomUUID()))
                .setThreshold(createSample())
                .setTriggered(Boolean.TRUE);
    }

    /**
     * Create a new reasonable pseudo-random <code>Condition</code>.
     *
     * @return New reasonable pseudo-random <code>Condition</code>.
     */
    public static Condition createCondition() {
        return createConditionBuilder().build();
    }

    /**
     * Create a builder for pseudo-random <code>FQDSN</code>.
     *
     * @return New builder for pseudo-random <code>FQDSN</code>.
     */
    public static FQDSN.Builder createFQDSNBuilder() {
        return new FQDSN.Builder()
                .setStatistic(MEAN_STATISTIC)
                .setService("service-" + UUID.randomUUID())
                .setMetric("metric-" + UUID.randomUUID())
                .setCluster("cluster-" + UUID.randomUUID());
    }

    /**
     * Create a new reasonable pseudo-random <code>FQDSN</code>.
     *
     * @return New reasonable pseudo-random <code>FQDSN</code>.
     */
    public static FQDSN createFQDSN() {
        return createFQDSNBuilder().build();
    }

    /**
     * Create a builder for pseudo-random <code>AggregatedData</code>.
     *
     * @return New builder for pseudo-random <code>AggregatedData</code>.
     */
    public static AggregatedData.Builder createAggregatedDataBuilder() {
        return new AggregatedData.Builder()
                .setFQDSN(createFQDSN())
                .setHost("host-" + UUID.randomUUID())
                .setValue(createSample())
                .setStart(DateTime.now())
                .setPeriod(Period.minutes(5))
                .setIsSpecified(true)
                .setSamples(Lists.newArrayList(createSample()))
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
                .setDimensions(ImmutableMap.of("host", "host-" + UUID.randomUUID()))
                .setData(ImmutableList.of(createAggregatedData()))
                .setConditions(ImmutableList.of())
                .setPeriod(Period.minutes(5))
                .setStart(DateTime.now());
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
    public static Quantity.Builder createSampleBuilder() {
        return new Quantity.Builder().setValue(Math.random()).setUnit(Unit.BIT);
    }

    /**
     * Create a new reasonable pseudo-random <code>Quantity</code>.
     *
     * @return New reasonable pseudo-random <code>Quantity</code>.
     */
    public static Quantity createSample() {
        return new Quantity.Builder().setValue(Math.random()).setUnit(Unit.BIT).build();
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

    private static final Function<Double, Quantity> CREATE_SAMPLE = new Function<Double, Quantity>() {
        @Override
        public Quantity apply(final Double input) {
            return new Quantity.Builder().setValue(input).setUnit(Unit.MILLISECOND).build();
        }
    };

    private TestBeanFactory() {}

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEAN_STATISTIC = STATISTIC_FACTORY.getStatistic("mean");
}
