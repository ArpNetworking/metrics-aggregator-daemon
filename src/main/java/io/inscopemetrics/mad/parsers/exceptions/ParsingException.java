/*
 * Copyright 2014 Brandon Arp
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

import com.arpnetworking.logback.annotations.Loggable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Exception thrown when a <code>Parser</code> fails to parse the data.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
@Loggable
public class ParsingException extends Exception {
    /**
     * Public constructor with a description.
     *
     * @param message Describes the exceptional condition.
     * @param offendingData The raw data that failed to parse.
     */
    public ParsingException(final String message, final byte[] offendingData) {
        super(message);
        _offendingData = offendingData;
    }

    /**
     * Public constructor with a description and cause.
     *
     * @param message Describes the exceptional condition.
     * @param offendingData The raw data that failed to parse.
     * @param cause Causing exception.
     */
    public ParsingException(final String message, final byte[] offendingData, final Throwable cause) {
        super(message, cause);
        _offendingData = offendingData;
    }

    public byte[] getOffendingData() {
        return _offendingData;
    }

    // TODO(barp): change this into a List or similar structure to ensure no modifications
    private final byte[] _offendingData;

    private static final long serialVersionUID = 1L;
}
