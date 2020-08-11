/*
 * Copyright 2020 Inscope Metrics
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
package com.arpnetworking.metrics.mad.parsers;

import com.arpnetworking.commons.test.BuildableTestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

/**
 * Tests for the {@link JsonToRecordParser} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class JsonToRecordParserTest {

    private final Supplier<JsonToRecordParser.Builder> _jsonParserBuilder = () -> new JsonToRecordParser.Builder()
            .setDefaultCluster("default-cluster")
            .setDefaultService("default-service")
            .setDefaultHost("default-host");

    @Test
    public void testBuilder() throws InvocationTargetException, IllegalAccessException {
        BuildableTestHelper.testBuild(
                _jsonParserBuilder.get(),
                JsonToRecordParser.class);
    }

    @Test
    public void testToString() {
        final String asString = _jsonParserBuilder.get().build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }
}
