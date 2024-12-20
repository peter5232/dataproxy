/*
 * Copyright 2024 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.plugin.odps.io;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yuexie
 * @date 2024/12/16 11:08
 **/
@Slf4j
public class DynamicSequenceInputStream extends InputStream {

    private final Queue<InputStream> streamQueue = new LinkedList<>();
    private volatile InputStream currentStream = null;

    private final AtomicBoolean isCompleted = new AtomicBoolean(false);

    private final Lock lock = new ReentrantLock(true);

    public void appendStream(InputStream stream) {

        try {
            lock.lock();

            if (isCompleted.get()) {
                throw new IllegalStateException("Stream is completed");
            }

            if (currentStream == null) {
                currentStream = stream;
            } else {
                if (!streamQueue.add(stream)) {
                    throw new IllegalStateException("Stream queue is full");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void setCompleted() {
        try {
            lock.lock();
            isCompleted.set(true);
        } finally {
            lock.unlock();
        }
    }



    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an {@code int} in the range {@code 0} to
     * {@code 255}. If no byte is available because the end of the stream
     * has been reached, the value {@code -1} is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     *
     * <p> A subclass must provide an implementation of this method.
     *
     * @return the next byte of data, or {@code -1} if the end of the
     * stream is reached.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public int read() throws IOException {

        if (currentStream == null) {
            return this.readNextStream();
        }

        int data = currentStream.read();

        if (data == -1) {
            currentStream.close();
            currentStream = null;
            return this.readNextStream();
        }
        return data;
    }

    @Override
    public void close() throws IOException {
        super.close();
        setCompleted();
        if (currentStream != null) {
            currentStream.close();
        }
        for (InputStream stream : streamQueue) {
            stream.close();
        }
    }

    private int readNextStream() throws IOException {

        // Promised: currentStream is null
        try {
            lock.lock();

            currentStream = streamQueue.poll();

            if (currentStream == null) {

                if (isCompleted.get()) {
                    return -1;
                }

                lock.unlock();
                TimeUnit.MILLISECONDS.sleep(100);
                return readNextStream();
            }

            return currentStream.read();

        } catch (InterruptedException e) {
            log.warn("Thread interrupted while waiting for next stream to read.", e);
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }

        return -1;
    }

}
