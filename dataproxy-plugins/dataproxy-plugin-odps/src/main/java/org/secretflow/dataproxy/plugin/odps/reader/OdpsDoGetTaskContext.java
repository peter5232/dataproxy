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

package org.secretflow.dataproxy.plugin.odps.reader;

import com.aliyun.odps.data.Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.secretflow.dataproxy.core.reader.Reader;
import org.secretflow.dataproxy.core.reader.Sender;
import org.secretflow.dataproxy.plugin.odps.config.TaskConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yuexie
 * @date 2024/12/5 15:20
 **/
@Slf4j
public class OdpsDoGetTaskContext implements AutoCloseable {

    private final Reader reader;
    private final Sender<Record> sender;

    private final VectorSchemaRoot root;

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    private Future<?> readFuture;

    private final AtomicBoolean hasNext = new AtomicBoolean(true);

    public OdpsDoGetTaskContext(TaskConfig taskConfig, VectorSchemaRoot root) {
        this.root = root;
        this.sender = getSender();
        this.reader = new OdpsTunnelRecordReader(taskConfig, this.sender, taskConfig.getContext().getDownloadSession());
    }

    public void start() {
        readFuture = executorService.submit(() -> {
            try {
                reader.read();
                sender.putOver();
                hasNext.set(false);
                log.info("read finished...");
            } catch (InterruptedException e) {
                log.error("read interrupted", e);
                Thread.currentThread().interrupt();
            }
        });
    }

    public void cancel() {
        if (readFuture != null && !readFuture.isDone()) {
            log.info("cancel read task...");
            readFuture.cancel(true);
        }
    }

    public boolean hasNext() {
        return hasNext.get();
    }

    public void putNextPatchData() {
        if ((!hasNext() && readFuture.isDone()) || readFuture.isCancelled() ) {
            return;
        }
        sender.send();
    }

    @Override
    public void close() throws Exception {
        this.cancel();
        executorService.shutdown();
    }

    private Sender<Record> getSender() {
        int estimatedRecordCount = 1_000;
        return new OdpsRecordSender(
                estimatedRecordCount,
                new LinkedBlockingQueue<>(estimatedRecordCount),
                root);
    }
}
