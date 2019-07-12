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

import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import java.util.Map;

/**
 * Implementation of <code>Comparator</code> which compares <code>Record</code>
 * instances ignoring the order of values in each metric.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class UnorderedRecordEquality {

    /**
     * Compare two <code>Record</code> instances ignoring the ordering of the
     * values in each metric.
     *
     * @param r1 First <code>Record</code> instance.
     * @param r2 Second <code>Record</code> instance.
     * @return True if and only if <code>Record</code> instances are equal
     * irregardless of the order of the values of each metric.
     */
    public static boolean equals(final Record r1, final Record r2) {
        if (!r1.getTime().equals(r2.getTime())
                || !r1.getAnnotations().equals(r2.getAnnotations())) {
            return false;
        }

        for (final Map.Entry<String, ? extends Metric> entry : r1.getMetrics().entrySet()) {
            if (!r2.getMetrics().containsKey(entry.getKey())) {
                return false;
            }
            final Metric m1 = entry.getValue();
            final Metric m2 = r2.getMetrics().get(entry.getKey());
            if (!m1.getType().equals(m2.getType())) {
                return false;
            }

            final Multiset<Quantity> v1 = HashMultiset.create(m1.getValues());
            final Multiset<Quantity> v2 = HashMultiset.create(m2.getValues());
            if (!v1.equals(v2)) {
                return false;
            }
        }

        return true;
    }

    private UnorderedRecordEquality() {}
}
