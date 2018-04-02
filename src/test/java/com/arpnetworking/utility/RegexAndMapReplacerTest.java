/**
 * Copyright 2018 Inscope Metrics, Inc
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

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * Tests for the {@link RegexAndMapReplacer} class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class RegexAndMapReplacerTest {
    @Test
    public void testNoMatch() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "wont match";
        final String replace = "$0";
        final String expected = "wont match";
        testExpression(pattern, input, replace, expected, ImmutableMap.of());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEscape() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test";
        final String replace = "${\\avariable}"; // \a is an invalid escape sequence
        final String result = RegexAndMapReplacer.replaceAll(pattern, input, replace, ImmutableMap.of()).getReplacement();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingClosingCurly() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test";
        final String replace = "${0"; // no ending }
        final String result = RegexAndMapReplacer.replaceAll(pattern, input, replace, ImmutableMap.of()).getReplacement();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEscapeAtEnd() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test";
        final String replace = "${0}\\"; // trailing \
        final String result = RegexAndMapReplacer.replaceAll(pattern, input, replace, ImmutableMap.of()).getReplacement();
    }

    @Test
    public void testNumericWithClosingCurly() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test";
        final String replace = "$0}";
        final String expected = "test}";
        testExpression(pattern, input, replace, expected, ImmutableMap.of());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidReplacementTokenMissingOpen() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test";
        final String replace = "$variable"; // replacement variable has no {
        final String result = RegexAndMapReplacer.replaceAll(pattern, input, replace, ImmutableMap.of()).getReplacement();
    }

    @Test
    public void testGroup0Replace() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test";
        final String replace = "$0";
        final String expected = "test";
        testExpression(pattern, input, replace, expected, ImmutableMap.of());
    }

    @Test
    public void testSingleMatchFullStaticReplace() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test";
        final String replace = "replace";
        final String expected = "replace";
        testExpression(pattern, input, replace, expected, ImmutableMap.of());
    }

    @Test
    public void testSingleMatchPartialStaticReplace() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test string";
        final String replace = "replace";
        final String expected = "replace string";
        testExpression(pattern, input, replace, expected, ImmutableMap.of());
    }

    @Test
    public void testSingleMatchPartialStaticReplacePrefix() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "some test string";
        final String replace = "replace";
        final String expected = "some replace string";
        testExpression(pattern, input, replace, expected, ImmutableMap.of());
    }

    @Test
    public void testSingleMatchPartialMultipleGroupNumberReplace() {
        final Pattern pattern = Pattern.compile("(test)/pattern/(foo)");
        final String input = "test/pattern/foo";
        final String replace = "this is a $1 pattern called $2";
        final String expected = "this is a test pattern called foo";
        testExpression(pattern, input, replace, expected, ImmutableMap.of());
    }

    @Test
    public void testSingleMatchPartialMultipleGroupNameReplace() {
        final Pattern pattern = Pattern.compile("(?<g1>test)/pattern/(?<g2>foo)");
        final String input = "test/pattern/foo";
        final String replace = "this is a ${g1} pattern called ${g2}";
        final String expected = "this is a test pattern called foo";
        testExpression(pattern, input, replace, expected, ImmutableMap.of());
    }

    @Test
    public void testSingleMatchPartialMultipleVariableReplace() {
        final Pattern pattern = Pattern.compile("test/pattern/foo");
        final String input = "test/pattern/foo";
        final String replace = "this is a ${g1} pattern called ${g2}";
        final String expected = "this is a test pattern called foo";
        testExpression(pattern, input, replace, expected, ImmutableMap.of("g1", "test", "g2", "foo"));
    }

    @Test
    public void testSingleMatchPartialMultipleVariableWithEscapeReplace() {
        final Pattern pattern = Pattern.compile("test/pattern/foo");
        final String input = "test/pattern/foo";
        final String replace = "this is a ${g1} pattern \\\\called\\\\ ${g2}";
        final String expected = "this is a test pattern \\called\\ foo";
        testExpression(pattern, input, replace, expected, ImmutableMap.of("g1", "test", "g2", "foo"));
    }

    @Test
    public void testSingleMatchPartialMultipleVariableWithEscapeTokenReplace() {
        final Pattern pattern = Pattern.compile("test/pattern/foo");
        final String input = "test/pattern/foo";
        final String replace = "this is a ${\\\\g1} pattern called ${g2}";
        final String expected = "this is a test pattern called foo";
        testExpression(pattern, input, replace, expected, ImmutableMap.of("\\g1", "test", "g2", "foo"));
    }

    @Test
    public void testSingleMatchPartialMultipleGroupNameOverridesVariablesReplace() {
        final Pattern pattern = Pattern.compile("(?<g1>test)/pattern/(?<g2>foo)");
        final String input = "test/pattern/foo";
        final String replace = "this is a ${g1} pattern called ${g2}";
        final String expected = "this is a test pattern called foo";
        testExpression(pattern, input, replace, expected, ImmutableMap.of("g1", "bad", "g2", "value"));
    }

    @Test
    public void testMultipleMatchFullStaticReplace() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "testtest";
        final String replace = "replace";
        final String expected = "replacereplace";
        testExpression(pattern, input, replace, expected, ImmutableMap.of());
    }

    @Test
    public void testMultipleMatchPartialStaticReplace() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test string test";
        final String replace = "replace";
        final String expected = "replace string replace";
        testExpression(pattern, input, replace, expected, ImmutableMap.of());
    }

    private void testExpression(final Pattern pattern, final String input, final String replace, final String expected,
            final ImmutableMap<String, String> variables) {
        final String result = RegexAndMapReplacer.replaceAll(pattern, input, replace, variables).getReplacement();
        Assert.assertEquals(expected, result);
        try {
            final String stockResult = pattern.matcher(input).replaceAll(replace);
            Assert.assertEquals(expected, stockResult);
        } catch (final IllegalArgumentException ignored) { }
    }
}
