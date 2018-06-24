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
package com.arpnetworking.utility;

import com.arpnetworking.configuration.jackson.JsonNodeLiteralSource;
import com.arpnetworking.configuration.jackson.StaticConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

/**
 * Tests for the <code>Configurator</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class ConfiguratorTest {

    @Test
    public void testOffer() throws Exception {
        final Configurator<TestLaunchable, String> configurator = new Configurator<>(TestLaunchable::new, String.class);
        configurator.offerConfiguration(new StaticConfiguration.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("\"foo\"").build())
                .build());

        final Optional<String> offeredConfiguration = configurator.getOfferedConfiguration();
        Assert.assertTrue(offeredConfiguration.isPresent());
        Assert.assertEquals("foo", offeredConfiguration.get());

        Assert.assertFalse(configurator.getLaunchable().isPresent());
        Assert.assertFalse(configurator.getConfiguration().isPresent());
    }

    @Test
    public void testOfferAndApply() throws Exception {
        final Configurator<TestLaunchable, String> configurator = new Configurator<>(TestLaunchable::new, String.class);
        configurator.offerConfiguration(new StaticConfiguration.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("\"foo\"").build())
                .build());
        configurator.applyConfiguration();

        final Optional<String> offeredConfiguration = configurator.getOfferedConfiguration();
        Assert.assertTrue(offeredConfiguration.isPresent());
        Assert.assertEquals("foo", offeredConfiguration.get());

        final Optional<String> configuration = configurator.getConfiguration();
        Assert.assertTrue(configuration.isPresent());
        Assert.assertEquals("foo", configuration.get());

        final Optional<TestLaunchable> launchable = configurator.getLaunchable();
        Assert.assertTrue(launchable.isPresent());
        Assert.assertEquals("foo", launchable.get().getValue());
        Assert.assertTrue(launchable.get().isRunning());
    }

    @Test(expected = Exception.class)
    public void testOfferFailure() throws Exception {
        final Configurator<TestLaunchable, String> configurator = new Configurator<>(TestLaunchable::new, String.class);
        configurator.offerConfiguration(new StaticConfiguration.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("{}").build())
                .build());
    }

    @Test
    public void testSecondOfferAndApply() throws Exception {
        final Configurator<TestLaunchable, String> configurator = new Configurator<>(TestLaunchable::new, String.class);
        configurator.offerConfiguration(new StaticConfiguration.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("\"foo\"").build())
                .build());
        configurator.applyConfiguration();

        final Optional<TestLaunchable> firstLaunchable = configurator.getLaunchable();
        Assert.assertTrue(firstLaunchable.isPresent());

        configurator.offerConfiguration(new StaticConfiguration.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("\"bar\"").build())
                .build());
        configurator.applyConfiguration();

        final Optional<String> offeredConfiguration = configurator.getOfferedConfiguration();
        Assert.assertTrue(offeredConfiguration.isPresent());
        Assert.assertEquals("bar", offeredConfiguration.get());

        final Optional<String> configuration = configurator.getConfiguration();
        Assert.assertTrue(configuration.isPresent());
        Assert.assertEquals("bar", configuration.get());

        final Optional<TestLaunchable> launchable = configurator.getLaunchable();
        Assert.assertTrue(launchable.isPresent());
        Assert.assertNotSame(firstLaunchable.get(), launchable.get());
        Assert.assertEquals("bar", launchable.get().getValue());
        Assert.assertTrue(launchable.get().isRunning());
    }

    @Test
    public void testSecondOfferFailure() throws Exception {
        final Configurator<TestLaunchable, String> configurator = new Configurator<>(TestLaunchable::new, String.class);
        configurator.offerConfiguration(new StaticConfiguration.Builder()
                .addSource(new JsonNodeLiteralSource.Builder().setSource("\"foo\"").build())
                .build());
        configurator.applyConfiguration();

        final Optional<TestLaunchable> firstLaunchable = configurator.getLaunchable();
        Assert.assertTrue(firstLaunchable.isPresent());

        try {
            configurator.offerConfiguration(new StaticConfiguration.Builder()
                    .addSource(new JsonNodeLiteralSource.Builder().setSource("{}").build())
                    .build());
            Assert.fail("Expected exception not thrown");
            // CHECKSTYLE.OFF: IllegalCatch - Defined by interface.
        } catch (final Exception e) {
            // CHECKSTYLE.ON: IllegalCatch
            final Optional<TestLaunchable> launchable = configurator.getLaunchable();
            Assert.assertTrue(launchable.isPresent());
            Assert.assertSame(firstLaunchable.get(), launchable.get());
            Assert.assertEquals("foo", launchable.get().getValue());
            Assert.assertTrue(launchable.get().isRunning());
        }
    }

    @Test
    public void testToString() {
        final String asString = new Configurator<>(TestLaunchable::new, String.class).toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    private static final class TestLaunchable implements Launchable {

        private TestLaunchable(final String value) {
            _value = value;
            _isRunning = false;
        }

        @Override
        public synchronized void launch() {
            _isRunning = true;
        }

        @Override
        public synchronized void shutdown() {
            _isRunning = false;
        }

        public boolean isRunning() {
            return _isRunning;
        }

        public String getValue() {
            return _value;
        }

        private volatile boolean _isRunning;
        private final String _value;
    }
}
