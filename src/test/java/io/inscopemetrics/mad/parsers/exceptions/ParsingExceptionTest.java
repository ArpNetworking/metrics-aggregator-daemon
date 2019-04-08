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
package io.inscopemetrics.mad.parsers.exceptions;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the ParsingException class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class ParsingExceptionTest {

    @Test
    public void testConstructorWithMessage() {
        final String expectedMessage = "This is a message";
        final ParsingException pe = new ParsingException(expectedMessage, new byte[0]);
        Assert.assertEquals(expectedMessage, pe.getMessage());
        Assert.assertNull(pe.getCause());
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        final String expectedMessage = "This is a message";
        final Throwable expectedCause = new NullPointerException("The cause");
        final ParsingException pe = new ParsingException(expectedMessage, new byte[0], expectedCause);
        Assert.assertEquals(expectedMessage, pe.getMessage());
        Assert.assertEquals(expectedCause, pe.getCause());
    }
}
