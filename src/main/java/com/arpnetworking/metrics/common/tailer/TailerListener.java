/**
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
 * Based on the Apache <code>TailerListener</code> but uses a <code>Tailer</code>
 * interface instead of a class for improved extensibility.
 *
 * @author Brandon Arp (barp at groupon dot com)
 */
public interface TailerListener {

    /**
     * The <code>Tailer</code> instance invokes this method during construction
     * giving the listening class a method of stopping the <code>Tailer</code>.
     *
     * @param tailer the <code>Tailer</code> instance.
     */
    void initialize(final Tailer tailer);

    /**
     * This method is called if the tailed file is not found.
     * <p>
     * <b>Note:</b> this is called from the <code>Tailer</code> thread.
     */
    void fileNotFound();

    /**
     * Called if a file rotation is detected.
     *
     * This method is called before the file is reopened, and fileNotFound may
     * be called if the new file has not been created yet.
     * <p>
     * <b>Note:</b> this is called from the <code>Tailer</code> thread.
     */
    void fileRotated();

    /**
     * Called if a file is successfully opened.
     *
     * <p>
     * <b>Note:</b> this is called from the <code>Tailer</code> thread.
     */
    void fileOpened();

    /**
     * Handles a line from a <code>Tailer</code>.
     * <p>
     * <b>Note:</b> this is called from the <code>Tailer</code> thread.
     * @param line the raw line.
     */
    void handle(final byte[] line);

    /**
     * Handles a <code>Throwable</code> encountered during tailing.
     * <p>
     * <b>Note:</b> this is called from the <code>Tailer</code> thread.
     * @param throwable the <code>Throwable</code>.
     */
    void handle(final Throwable throwable);

}
