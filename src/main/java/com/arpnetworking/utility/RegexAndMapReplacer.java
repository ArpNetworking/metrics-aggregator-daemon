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
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A regex replacement utility that can also replace tokens not found in the regex.
 *
 * ${name} in the replace string is replaced by the pattern's named capture, or by the value of the variable
 * with that name from the variables map
 * \ is used as an escape character and may be used to escape '$', '{', and '}' characters so that they will
 * be treated as literals
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class RegexAndMapReplacer {
    /**
     * Replaces all instances of $n (where n is 0-9) with regex match groups, ${var} with regex capture group or variable (from map) 'var'.
     *
     * @param pattern pattern to use
     * @param input input string to match against
     * @param replace replacement string
     * @param variables map of variables to include
     * @return a string with replacement tokens replaced
     */
    public static Replacement replaceAll(
            final Pattern pattern,
            final String input,
            final String replace,
            final LinkedHashMap<String, Map<String, String>> variables) {
        final Matcher matcher = pattern.matcher(input);
        boolean found = matcher.find();
        if (found) {
            final Map<String, ImmutableList.Builder<String>> variablesUsed = Maps.newHashMap();
            final StringBuilder builder = new StringBuilder();
            int lastMatchedIndex = 0;
            do {
                builder.append(input.substring(lastMatchedIndex, matcher.start()));
                lastMatchedIndex = matcher.end();
                appendReplacement(matcher, replace, builder, variables, variablesUsed);
                found = matcher.find();
            } while (found);
            // Append left-over string after the matches
            if (lastMatchedIndex < input.length() - 1) {
                builder.append(input.substring(lastMatchedIndex, input.length()));
            }
            final ImmutableMap<String, ImmutableList<String>> immutableVars = variablesUsed.entrySet()
                    .stream()
                    .collect(
                            Collectors.collectingAndThen(
                                    Collectors.toMap(ImmutableMap.Entry::getKey, entry -> entry.getValue().build()),
                                    ImmutableMap::copyOf));
            return new Replacement(builder.toString(), immutableVars);
        }
        return new Replacement(input, ImmutableMap.of());
    }

    private static void appendReplacement(
            final Matcher matcher,
            final String replacement,
            final StringBuilder replacementBuilder,
            final LinkedHashMap<String, Map<String, String>> variables,
            final Map<String, ImmutableList.Builder<String>> variablesUsed) {
        final StringBuilder tokenBuilder = new StringBuilder();
        int x = -1;
        while (x < replacement.length() - 1) {
            x++;
            final char c = replacement.charAt(x);
            if (c == '\\') {
                x++;
                processEscapedCharacter(replacement, x, replacementBuilder);
            } else {
                if (c == '$') {
                    x += writeReplacementToken(replacement, x, replacementBuilder, matcher, variables, tokenBuilder, variablesUsed);
                } else {
                    replacementBuilder.append(c);
                }
            }
        }
    }

    private static void processEscapedCharacter(final String replacement, final int x, final StringBuilder builder) {
        if (x >= replacement.length()) {
            throw new IllegalArgumentException(
                    String.format("Improper escaping in replacement, must not have trailing '\\' at col %d: %s", x, replacement));
        }
        final Character c = replacement.charAt(x);
        if (c == '\\' || c == '$' || c == '{' || c == '}') {
            builder.append(c);
        } else {
            throw new IllegalArgumentException(
                    String.format("Improperly escaped '%s' in replacement at col %d: %s", c, x, replacement));
        }
    }

    private static int writeReplacementToken(
            final String replacement,
            final int offset,
            final StringBuilder output,
            final Matcher matcher,
            final LinkedHashMap<String, Map<String, String>> variables,
            final StringBuilder tokenBuilder,
            final Map<String, ImmutableList.Builder<String>> variablesUsed) {
        boolean inReplaceBrackets = false;
        boolean tokenNumeric = true;
        tokenBuilder.setLength(0);  // reset the shared builder
        int x = offset + 1;
        char c = replacement.charAt(x);

        // Optionally consume the opening brace
        if (c == '{') {
            inReplaceBrackets = true;
            x++;
            c = replacement.charAt(x);
        }

        if (inReplaceBrackets) {
            // Consume until we hit the }
            while (x < replacement.length() - 1 && c != '}') {
                if (c == '\\') {
                    x++;
                    processEscapedCharacter(replacement, x, tokenBuilder);
                } else {
                    tokenBuilder.append(c);
                }
                if (tokenNumeric && !Character.isDigit(c)) {
                    tokenNumeric = false;
                }
                x++;
                c = replacement.charAt(x);
            }
            if (c != '}') {
                throw new IllegalArgumentException("Invalid replacement token, expected '}' at col " + x + ": " + replacement);
            }
            x++; // Consume the }
            output.append(getReplacement(matcher, tokenBuilder.toString(), variables, variablesUsed));
        } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid replacement token, tokens must be surrounded by { } at col %d: %s",
                                x,
                                replacement));
        }
        return x - offset - 1;
    }

    private static String getReplacement(
            final Matcher matcher,
            final String replaceToken,
            final LinkedHashMap<String, Map<String, String>> variables,
            final Map<String, ImmutableList.Builder<String>> variablesUsed) {
        final int prefixSeparatorIndex = replaceToken.indexOf(':');
        final String prefix;
        final String variableName;
        if (prefixSeparatorIndex == replaceToken.length() - 1) {
            throw new IllegalArgumentException(
                    String.format("found prefix in variable replacement, but no variable name found: '%s'",
                    replaceToken));
        }
        if (prefixSeparatorIndex != -1) {
            prefix = replaceToken.substring(0, prefixSeparatorIndex);
            variableName = replaceToken.substring(prefixSeparatorIndex + 1);
        } else {
            prefix = "";
            variableName = replaceToken;
        }
        if (prefix.isEmpty()) {
            return replaceNonPrefix(matcher, variables, variablesUsed, variableName);

        } else {
            return replacePrefix(matcher, variables, variablesUsed, prefix, variableName);
        }
    }

    private static String replacePrefix(
            final Matcher matcher,
            final LinkedHashMap<String, Map<String, String>> variables,
            final Map<String, ImmutableList.Builder<String>> variablesUsed,
            final String prefix,
            final String variableName) {
        if (prefix.equals("capture")) {
            if (isNumeric(variableName)) {
                final Integer groupIndex = Integer.valueOf(variableName);
                if (groupIndex >= 0 && groupIndex <= matcher.groupCount()) {
                    return matcher.group(groupIndex);
                }
            }

            try {
                return matcher.group(variableName);
            } catch (final IllegalArgumentException ignored) { // No group with this name
                return "";
            }
        }

        // Only record variables that are not captures
        variablesUsed.computeIfAbsent(prefix, key -> ImmutableList.builder()).add(variableName);

        final Map<String, String> variableMap = variables.get(prefix);
        if (variableMap == null) {
            throw new IllegalArgumentException(String.format("could not find map for variables with prefix '%s'", prefix));
        }
        return variableMap.getOrDefault(variableName, "");
    }

    private static String replaceNonPrefix(
            final Matcher matcher,
            final LinkedHashMap<String, Map<String, String>> variables,
            final Map<String, ImmutableList.Builder<String>> variablesUsed,
            final String variableName) {
        // First try to check against the capture group number
        if (isNumeric(variableName)) {
            final int replaceGroup = Integer.parseInt(variableName);
            if (replaceGroup >= 0 && replaceGroup <= matcher.groupCount()) {
                return matcher.group(replaceGroup);
            }
        }
        // Then try the capture group with the name
        try {
            return matcher.group(variableName);
        } catch (final IllegalArgumentException e) { // No group with this name
            // Walk through the variable maps in order to find the first match
            for (final Map.Entry<String, Map<String, String>> entry : variables.entrySet()) {
                final Map<String, String> variableMap = entry.getValue();
                final String replacement = variableMap.get(variableName);
                if (replacement != null) {
                    variablesUsed.computeIfAbsent(entry.getKey(), key -> ImmutableList.builder()).add(variableName);
                    return replacement;
                }
            }
        }
        return "";
    }

    private static boolean isNumeric(final String string) {
        boolean isNumeric = true;
        for (int x = 0; x < string.length(); x++) {
            if (!Character.isDigit(string.charAt(x))) {
                isNumeric = false;
                break;
            }
        }
        return isNumeric;
    }

    private RegexAndMapReplacer() { }

    /**
     * Describes the replacement string and variables used in it's creation.
     *
     * The "replacement" field is the resulting string.
     * The "variablesMatched" field is a list of input variables that were matched, in prefix:variable form
     */
    public static final class Replacement {
        public String getReplacement() {
            return _replacement;
        }

        public ImmutableMap<String, ImmutableList<String>> getVariablesMatched() {
            return _variablesMatched;
        }

        private Replacement(final String replacement, final ImmutableMap<String, ImmutableList<String>> variablesMatched) {

            _replacement = replacement;
            _variablesMatched = variablesMatched;
        }

        private final String _replacement;
        private final ImmutableMap<String, ImmutableList<String>> _variablesMatched;
    }
}
