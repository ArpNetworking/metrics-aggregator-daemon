/*
 * Copyright 2019 Dropbox.com
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
package com.arpnetworking.test;

import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.common.parsers.exceptions.ParsingException;
import com.arpnetworking.metrics.mad.model.Record;

import java.util.Collections;
import java.util.List;

/**
 * Trivial {@link Parser} that converts Strings to Record with the string id.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class StringToRecordParser implements Parser<List<Record>, String> {

    @Override
    public List<Record> parse(final String data) throws ParsingException {
        if (data.length() < 1) {
            // ID cannot be empty
            return Collections.singletonList(recordWithID(" "));
        } else {
            return Collections.singletonList(recordWithID(data));
        }
    }

    /**
     * Creates a {@link Record} with the passed in ID.
     *
     * @param id the id
     * @return the created {@link Record}
     */
    public static Record recordWithID(final String id) {
        return TestBeanFactory.createRecordBuilder().setId(id).build();
    }

    /**
     * Accepts on object expected of type {@link Record} and returns its ID.
     *
     * @param o the object
     * @return the ID
     */
    public static String recordID(final Object o) {
        if (!(o instanceof Record)) {
            throw new IllegalArgumentException("Expected object of type record");
        }
        final Record record = (Record) o;
        return record.getId();
    }
}
