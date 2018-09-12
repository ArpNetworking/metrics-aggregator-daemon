package com.arpnetworking.metrics.common.sources;

import com.arpnetworking.metrics.mad.parsers.PrometheusToRecordParser;

public final class PrometheusHttpSource extends HttpSource{
    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    protected PrometheusHttpSource(HttpSource.Builder<?, ? extends HttpSource> builder) {
        super(builder);
    }

    /**
     * Name of the actor created to receive the HTTP Posts.
     */
    public static final String ACTOR_NAME = "prometheus";

    /**
     * PrometheusHttpSource {@link BaseSource.Builder} implementation.
     */
    public static final class Builder extends HttpSource.Builder<Builder, PrometheusHttpSource> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(PrometheusHttpSource::new);
            setActorName(ACTOR_NAME);
            setParser(new PrometheusToRecordParser());
        }

        @Override
        protected Builder self() {
            return this;
        }
    }

}
