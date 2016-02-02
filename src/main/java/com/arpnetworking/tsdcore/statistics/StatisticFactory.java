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
package com.arpnetworking.tsdcore.statistics;

import com.arpnetworking.utility.InterfaceDatabase;
import com.arpnetworking.utility.ReflectionsDatabase;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

/**
 * Creates statistics.
 *
 * @author Brandon Arp (barp at groupon dot com)
 * @author Ville Koskela (vkoskela at groupon dot com)
 */
public class StatisticFactory {

    /**
     * Get a statistic by name.
     *
     * @param name The name of the desired statistic.
     * @return A new <code>Statistic</code>.
     */
    public Statistic getStatistic(final String name) {
        final Optional<Statistic> statistic = tryGetStatistic(name);
        if (!statistic.isPresent()) {
            throw new IllegalArgumentException(String.format("Invalid statistic name; name=%s", name));
        }
        return statistic.get();
    }

    /**
     * Get a statistic by name.
     *
     * @param name The name of the desired statistic.
     * @return A new <code>Statistic</code>.
     */
    public Optional<Statistic> tryGetStatistic(final String name) {
        return Optional.fromNullable(STATISTICS_BY_NAME_AND_ALIAS.get(name));
    }

    /**
     * Get all registered <code>Statistic</code> instances.
     *
     * @return A new <code>Statistic</code>.
     */
    public ImmutableSet<Statistic> getAllStatistics() {
        return ALL_STATISTICS;
    }

    /**
     * Creates a statistic from a name.
     *
     * @param statistic The name of the desired statistic.
     * @return A new <code>Statistic</code>.
     * @deprecated Use <code>getStatistic</code> instead.
     */
    @Deprecated
    public Optional<Statistic> createStatistic(final String statistic) {
        return Optional.fromNullable(STATISTICS_BY_NAME_AND_ALIAS.get(statistic));
    }

    private static void checkedPut(final Map<String, Statistic> map, final Statistic statistic, final String key) {
        final Statistic existingStatistic =  map.get(key);
        if (existingStatistic != null) {
            if (!existingStatistic.equals(statistic)) {
                LOGGER.error(String.format(
                        "Statistic already registered; key=%s, existing=%s, new=%s",
                        key,
                        existingStatistic,
                        statistic));
            }
            return;
        }
        map.put(key, statistic);
    }

    private static final ImmutableMap<String, Statistic> STATISTICS_BY_NAME_AND_ALIAS;
    private static final ImmutableSet<Statistic> ALL_STATISTICS;
    private static final InterfaceDatabase INTERFACE_DATABASE = ReflectionsDatabase.newInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticFactory.class);

    static {
        // NOTE: Do not put log messages in static blocks since they can lock the logger thread!
        final Map<String, Statistic> statisticByNameAndAlias = Maps.newHashMap();
        final Set<Statistic> allStatistics = Sets.newHashSet();
        final Set<Class<? extends Statistic>> statisticClasses = INTERFACE_DATABASE.findClassesWithInterface(Statistic.class);
        for (final Class<? extends Statistic> statisticClass : statisticClasses) {
            if (!statisticClass.isInterface() && !Modifier.isAbstract(statisticClass.getModifiers())) {
                try {
                    final Constructor<? extends Statistic> constructor = statisticClass.getDeclaredConstructor();
                    if (!constructor.isAccessible()) {
                        constructor.setAccessible(true);
                    }
                    final Statistic statistic = constructor.newInstance();
                    allStatistics.add(statistic);
                    checkedPut(statisticByNameAndAlias, statistic, statistic.getName());
                    for (final String alias : statistic.getAliases()) {
                        checkedPut(statisticByNameAndAlias, statistic, alias);
                    }
                } catch (final InvocationTargetException | NoSuchMethodException
                        | InstantiationException | IllegalAccessException e) {
                    LOGGER.warn(String.format("Unable to load statistic; class=%s", statisticClass), e);
                }
            }
        }
        STATISTICS_BY_NAME_AND_ALIAS = ImmutableMap.copyOf(statisticByNameAndAlias);
        ALL_STATISTICS = ImmutableSet.copyOf(allStatistics);
    }
}
