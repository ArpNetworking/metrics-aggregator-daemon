package com.arpnetworking.metrics.mad.actors;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.incubator.impl.TsdPeriodicMetrics;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

public class PeriodicMetricsCloser extends AbstractActorWithTimers {
    public static Props props(final TsdPeriodicMetrics periodicMetrics) {
        return Props.create(PeriodicMetricsCloser.class, () -> new PeriodicMetricsCloser(periodicMetrics));
    }

    @Inject
    public PeriodicMetricsCloser(final TsdPeriodicMetrics periodicMetrics) {
        _periodicMetrics = periodicMetrics;
        timers().startPeriodicTimer(COLLECT_KEY, COLLECT_MESSAGE, FiniteDuration.apply(500, TimeUnit.MILLISECONDS));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals(COLLECT_MESSAGE, message -> {
                    _periodicMetrics.run();
                })
                .build();
    }

    private final TsdPeriodicMetrics _periodicMetrics;

    private static final String COLLECT_KEY = "collect";
    private static final String COLLECT_MESSAGE = "collect";
}
