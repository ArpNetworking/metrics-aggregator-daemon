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
package com.arpnetworking.metrics.common.tailer;

/**
 * Based on the Apache {@link TailerListener} but uses a {@link Tailer}
 * interface instead of a class for improved extensibility.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public interface TailerListener {

    /**
     * The {@link Tailer} instance invokes this method during construction
     * giving the listening class a method of stopping the {@link Tailer}.
     *
     * @param tailer the {@link Tailer} instance.
     */
    void initialize(Tailer tailer);

    /**
     * This method is called if the tailed file is not found.
     * <p>
     * <b>Note:</b> this is called from the {@link Tailer} thread.
     */
    void fileNotFound();

    /**
     * Called if a file rotation is detected.
     *
     * This method is called before the file is reopened, and fileNotFound may
     * be called if the new file has not been created yet.
     * <p>
     * <b>Note:</b> this is called from the {@link Tailer} thread.
     */
    void fileRotated();

    /**
     * Called if a file is successfully opened.
     *
     * <p>
     * <b>Note:</b> this is called from the {@link Tailer} thread.
     */
    void fileOpened();

    /**
     * Handles a line from a {@link Tailer}.
     * <p>
     * <b>Note:</b> this is called from the {@link Tailer} thread.
     * @param line the raw line.
     */
    void handle(byte[] line);

    /**
     * Handles a {@link Throwable} encountered during tailing.
     * <p>
     * <b>Note:</b> this is called from the {@link Tailer} thread.
     * @param throwable the {@link Throwable}.
     */
    void handle(Throwable throwable);

}
