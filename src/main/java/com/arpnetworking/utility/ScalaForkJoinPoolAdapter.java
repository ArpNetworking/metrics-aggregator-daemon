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

import scala.concurrent.forkjoin.ForkJoinPool;

/**
 * This is a partial adapter to enable instrumentation of Scala's
 * {@code ForkJoinPool} as if it were a Java {@code ForkJoinPool}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class ScalaForkJoinPoolAdapter extends InstrumentingPoolAdapter {

    /**
     * Public constructor.
     *
     * @param scalaForkJoinPool pool to measure
     */
    public ScalaForkJoinPoolAdapter(final ForkJoinPool scalaForkJoinPool) {
        _scalaForkJoinPool = scalaForkJoinPool;
    }

    private final ForkJoinPool _scalaForkJoinPool;

    @Override
    public int getParallelism() {
        return _scalaForkJoinPool.getParallelism();
    }

    @Override
    public int getPoolSize() {
        return _scalaForkJoinPool.getPoolSize();
    }

    @Override
    public int getRunningThreadCount() {
        return _scalaForkJoinPool.getRunningThreadCount();
    }

    @Override
    public int getActiveThreadCount() {
        return _scalaForkJoinPool.getActiveThreadCount();
    }

    @Override
    public boolean isQuiescent() {
        return _scalaForkJoinPool.isQuiescent();
    }

    @Override
    public long getStealCount() {
        return _scalaForkJoinPool.getStealCount();
    }

    @Override
    public long getQueuedTaskCount() {
        return _scalaForkJoinPool.getQueuedTaskCount();
    }

    @Override
    public int getQueuedSubmissionCount() {
        return _scalaForkJoinPool.getQueuedSubmissionCount();
    }

    @Override
    public boolean hasQueuedSubmissions() {
        return _scalaForkJoinPool.hasQueuedSubmissions();
    }

    @Override
    public String toString() {
        return _scalaForkJoinPool.toString();
    }
}
