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
package com.arpnetworking.configuration.jackson;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the <code>JsonNodeLiteralSource</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class JsonNodeLiteralSourceTest {

    @Test
    public void testValidJson() {
        final JsonNodeLiteralSource source = new JsonNodeLiteralSource.Builder()
                .setSource("{\"foo\":\"bar\"}")
                .build();
        Assert.assertTrue(source.getJsonNode().isPresent());
        Assert.assertEquals("bar", source.getValue("foo").get().textValue());
        Assert.assertFalse(source.getValue("does-not-exist").isPresent());
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidJson() {
        new JsonNodeLiteralSource.Builder()
                .setSource("json=not-valid")
                .build();
    }
}
