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
package io.inscopemetrics.mad.sources;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.observer.ObservableDelegate;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.google.common.base.Suppliers;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Abstract base source class. This class is thread safe.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public abstract class BaseSource implements Source {

    @Override
    public void attach(final Observer observer) {
        _observable.attach(observer);
    }

    @Override
    public void detach(final Observer observer) {
        _observable.detach(observer);
    }

    /**
     * Dispatch an event to all attached {@code Observer} instances.
     *
     * @param event the event to dispatch
     */
    protected void notify(final Object event) {
        _observable.notify(this, event);
    }

    public String getName() {
        return _name;
    }

    /**
     * Return the metric safe name of this source.
     *
     * @return the metric safe name of this source
     */
    public String getMetricSafeName() {
        return _metricSafeNameSupplier.get();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("name", _name)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    /**
     * Protected constructor.
     *
     * @param builder instance of {@link Builder}
     */
    protected BaseSource(final Builder<?, ?> builder) {
        _name = builder._name;
        _metricSafeNameSupplier = Suppliers.memoize(() ->
                getName().replaceAll("[/\\. \t\n]", "_")
        );
    }

    private final String _name;
    private final Supplier<String> _metricSafeNameSupplier;
    private final ObservableDelegate _observable = ObservableDelegate.newInstance();

    /**
     * BaseSource {@code Builder} implementation.
     *
     * @param <B> type of the builder
     * @param <S> type of the source to build
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
     */
    protected abstract static class Builder<B extends Builder<B, S>, S extends Source> extends OvalBuilder<S> {

        /**
         * Sets name. Required. Cannot be null or empty.
         *
         * @param value the name
         * @return this instance of {@link Builder}
         */
        public final B setName(final String value) {
            _name = value;
            return self();
        }

        /**
         * Called by setters to always return appropriate subclass of
         * {@link Builder}, even from setters of base class.
         *
         * @return instance with correct {@link Builder} class type
         */
        protected abstract B self();

        /**
         * Protected constructor for subclasses.
         *
         * @param targetConstructor the constructor for the concrete type to be created by this builder
         */
        protected Builder(final Function<B, S> targetConstructor) {
            super(targetConstructor);
        }

        @NotNull
        @NotEmpty
        private String _name;
    }
}
