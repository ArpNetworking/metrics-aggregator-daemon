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

import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Observer to collect the values it observes in a collection.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public final class CollectObserver implements Observer {
    private List<String> _collection = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void notify(final Observable observable, final Object object) {
        if (object instanceof String) {
            _collection.add((String) object);
        }
    }

    /**
     * Return the collection.
     *
     * @return the collection of observed values
     */
    public List<String> getCollection() {
        return _collection;
    }
}
