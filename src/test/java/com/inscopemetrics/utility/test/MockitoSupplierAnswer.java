/*
 * Copyright 2016 Smartsheet
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
package com.inscopemetrics.utility.test;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.function.Supplier;

/**
 * Uses a supplier to create an Answer.
 *
 * @param <T> The type of the return value.
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public final class MockitoSupplierAnswer<T> implements Answer<T> {
    @Override
    public T answer(final InvocationOnMock invocation) throws Throwable {
        return _supplier.get();
    }

    private MockitoSupplierAnswer(final Supplier<T> supplier) {
        _supplier = supplier;
    }

    private final Supplier<T> _supplier;

    /**
     * Factory method.
     *
     * @param supplier Supplier to execute for answers.
     * @param <T> Type of the return value.
     * @return A new {@link Answer}.
     */
    public static <T> MockitoSupplierAnswer<T> create(final Supplier<T> supplier) {
        return new MockitoSupplierAnswer<>(supplier);
    }
}
