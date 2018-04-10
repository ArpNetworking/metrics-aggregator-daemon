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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
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
        final String result = RegexAndMapReplacer.replaceAll(pattern, input, replace, new LinkedHashMap<>()).getReplacement();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingClosingCurly() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test";
        final String replace = "${0"; // no ending }
        final String result = RegexAndMapReplacer.replaceAll(pattern, input, replace, new LinkedHashMap<>()).getReplacement();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEscapeAtEnd() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test";
        final String replace = "${0}\\"; // trailing \
        final String result = RegexAndMapReplacer.replaceAll(pattern, input, replace, new LinkedHashMap<>()).getReplacement();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNoVariableName() {
        final Pattern pattern = Pattern.compile("test");
        final String input = "test";
        final String replace = "${prefix:}"; // variable prefix, but no name
        final String result = RegexAndMapReplacer.replaceAll(pattern, input, replace, new LinkedHashMap<>()).getReplacement();
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
        final String result = RegexAndMapReplacer.replaceAll(pattern, input, replace, new LinkedHashMap<>()).getReplacement();
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
    public void testMatchPrefix() {
        final Pattern pattern = Pattern.compile("input_string_(?<var>.*)_(.*)");
        final String input = "input_string_ending_ending2";
        final String replace = "${prefix1:var}_${prefix2:var}_${capture:var}_${capture:2}";
        final String expected = "var1_var2_ending_ending2";
        final LinkedHashMap<String, Map<String, String>> variables = new LinkedHashMap<>();
        variables.put("prefix1", ImmutableMap.of("var", "var1"));
        variables.put("prefix2", ImmutableMap.of("var", "var2"));
        final RegexAndMapReplacer.Replacement replacement = testExpression(pattern, input, replace, expected, variables);
        Assert.assertEquals(ImmutableList.of("prefix1:var", "prefix2:var"), replacement.getVariablesMatched());
    }

    @Test
    public void testMatchPrecedence() {
        final Pattern pattern = Pattern.compile("input_string_(?<var>.*)");
        final String input = "input_string_ending";
        final String replace = "${var}_${var2}_${var3}";
        final String expected = "ending_1-var2_2-var3";
        final LinkedHashMap<String, Map<String, String>> variables = new LinkedHashMap<>();
        variables.put("prefix1", ImmutableMap.of("var", "1-var", "var2", "1-var2"));
        variables.put("prefix2", ImmutableMap.of("var", "2-var", "var2", "2-var2", "var3", "2-var3"));
        final RegexAndMapReplacer.Replacement replacement = testExpression(pattern, input, replace, expected, variables);
        Assert.assertEquals(ImmutableList.of("prefix1:var2", "prefix2:var3"), replacement.getVariablesMatched());
    }

    @Test
    public void testMatchMissingVars() {
        final Pattern pattern = Pattern.compile("input_string");
        final String input = "input_string_ending";
        final String replace = "${prefix1:missing}1${capture:missing}2${missing}";
        final String expected = "12_ending";
        final LinkedHashMap<String, Map<String, String>> variables = new LinkedHashMap<>();
        variables.put("prefix1", ImmutableMap.of("var", "var1"));
        testExpression(pattern, input, replace, expected, variables);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMatchInvalidPrefix() {
        final Pattern pattern = Pattern.compile("input_string_(?<var>.*)");
        final String input = "input_string_ending";
        final String replace = "${notfound:var}_value";
        final String expected = "illegal arg exception";
        final LinkedHashMap<String, Map<String, String>> variables = new LinkedHashMap<>();
        variables.put("prefix1", ImmutableMap.of("var", "var1"));
        testExpression(pattern, input, replace, expected, variables);
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

    private RegexAndMapReplacer.Replacement testExpression(
            final Pattern pattern,
            final String input,
            final String replace,
            final String expected,
            final ImmutableMap<String, String> variables) {
        final LinkedHashMap<String, Map<String, String>> variablesMap = new LinkedHashMap<>();
        variablesMap.put("myvars", variables);
        return testExpression(pattern, input, replace, expected, variablesMap);
    }

    private RegexAndMapReplacer.Replacement testExpression(
            final Pattern pattern,
            final String input,
            final String replace,
            final String expected,
            final LinkedHashMap<String, Map<String, String>> variables) {
        final RegexAndMapReplacer.Replacement replacement = RegexAndMapReplacer.replaceAll(pattern, input, replace, variables);
        final String result = replacement.getReplacement();
        Assert.assertEquals(expected, result);
        try {
            final String stockResult = pattern.matcher(input).replaceAll(replace);
            Assert.assertEquals(expected, stockResult);
        } catch (final IllegalArgumentException ignored) { }
        return replacement;
    }
}
