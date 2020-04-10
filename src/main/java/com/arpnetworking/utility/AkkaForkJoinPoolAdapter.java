/*
 * Copyright 2017 Smartsheet
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

/**
 * This is a partial adapter to enable instrumentation of Akka's
 * {@code ForkJoinPool} as if it were a Java {@code ForkJoinPool}.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public final class AkkaForkJoinPoolAdapter extends InstrumentingPoolAdapter {
    /**
     * Public constructor.
     *
     * @param akkaForkJoinPool pool to measure
     */
    public AkkaForkJoinPoolAdapter(final akka.dispatch.forkjoin.ForkJoinPool akkaForkJoinPool) {
        _akkaForkJoinPool = akkaForkJoinPool;
    }

    private final akka.dispatch.forkjoin.ForkJoinPool _akkaForkJoinPool;

    @Override
    public int getParallelism() {
        return _akkaForkJoinPool.getParallelism();
    }

    @Override
    public int getPoolSize() {
        return _akkaForkJoinPool.getPoolSize();
    }

    @Override
    public int getRunningThreadCount() {
        return _akkaForkJoinPool.getRunningThreadCount();
    }

    @Override
    public int getActiveThreadCount() {
        return _akkaForkJoinPool.getActiveThreadCount();
    }

    @Override
    public boolean isQuiescent() {
        return _akkaForkJoinPool.isQuiescent();
    }

    @Override
    public long getStealCount() {
        return _akkaForkJoinPool.getStealCount();
    }

    @Override
    public long getQueuedTaskCount() {
        return _akkaForkJoinPool.getQueuedTaskCount();
    }

    @Override
    public int getQueuedSubmissionCount() {
        return _akkaForkJoinPool.getQueuedSubmissionCount();
    }

    @Override
    public boolean hasQueuedSubmissions() {
        return _akkaForkJoinPool.hasQueuedSubmissions();
    }

    @Override
    public String toString() {
        return _akkaForkJoinPool.toString();
    }
}
