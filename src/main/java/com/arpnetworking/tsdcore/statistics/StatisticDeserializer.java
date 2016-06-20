/**
 * Copyright 2015 Groupon.com
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
package com.arpnetworking.tsdcore.statistics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

/**
 * Jackson <code>JsonDeserializer</code> implementation for <code>Statistic</code> using <code>StatisticFactory</code>.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class StatisticDeserializer extends JsonDeserializer<Statistic> {

    /**
     * Create a new instance of <code>JsonDeserializer&lt;Statistic&gt;</code>.
     *
     * @return New instance of <code>JsonDeserializer&lt;Statistic&gt;</code>.
     */
    public static JsonDeserializer<Statistic> newInstance() {
        return new StatisticDeserializer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statistic deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
        final String statisticNameOrAlias = parser.getValueAsString();
        return STATISTIC_FACTORY.getStatistic(statisticNameOrAlias);
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
}
