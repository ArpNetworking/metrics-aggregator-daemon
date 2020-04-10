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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An adapter used to facilitate the instrumentation of work pools.  Allows for the use of
 * functions that measure the state of the pool, but will throw an exception on any methods
 * that submit work units or query work units.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public abstract class InstrumentingPoolAdapter extends java.util.concurrent.ForkJoinPool {
    @Override
    public <T> T invoke(final ForkJoinTask<T> task) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public void execute(final ForkJoinTask<?> task) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public void execute(final Runnable task) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public <T> ForkJoinTask<T> submit(final ForkJoinTask<T> task) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public <T> ForkJoinTask<T> submit(final Callable<T> task) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public <T> T invokeAny(
            final Collection<? extends Callable<T>> tasks,
            final long timeout,
            final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public <T> ForkJoinTask<T> submit(final Runnable task, final T result) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public ForkJoinTask<?> submit(final Runnable task) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public <T> List<Future<T>> invokeAll(
            final Collection<? extends Callable<T>> tasks,
            final long timeout,
            final TimeUnit unit)
            throws InterruptedException {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public ForkJoinWorkerThreadFactory getFactory() {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public boolean getAsyncMode() {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public List<Runnable> shutdownNow() {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public boolean isTerminated() {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public boolean isTerminating() {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public boolean isShutdown() {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public boolean awaitQuiescence(final long timeout, final TimeUnit unit) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    public abstract int getParallelism();

    @Override
    public abstract int getPoolSize();

    @Override
    public abstract int getRunningThreadCount();

    @Override
    public abstract int getActiveThreadCount();

    @Override
    public abstract boolean isQuiescent();

    @Override
    public abstract long getStealCount();

    @Override
    public abstract long getQueuedTaskCount();

    @Override
    public abstract int getQueuedSubmissionCount();

    @Override
    public abstract boolean hasQueuedSubmissions();

    @Override
    public abstract String toString();

    @Override
    protected ForkJoinTask<?> pollSubmission() {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    protected int drainTasksTo(final Collection<? super ForkJoinTask<?>> c) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(final Callable<T> callable) {
        throw new UnsupportedOperationException("This adapter only supports instrumentation");
    }
}
