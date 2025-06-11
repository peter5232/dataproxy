package org.secretflow.dataproxy.plugin.database.reader;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.secretflow.dataproxy.plugin.database.config.TaskConfig;
import org.secretflow.dataproxy.core.reader.Sender;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.secretflow.dataproxy.plugin.database.reader.DatabaseRecordReader;
import org.secretflow.dataproxy.plugin.database.reader.DatabaseRecordSender;
import org.secretflow.dataproxy.plugin.database.utils.Record;

@Slf4j
public class DatabaseDoGetTaskContext implements AutoCloseable{

    private final Sender<Record> sender;

    private final TaskConfig taskConfig;
    private final VectorSchemaRoot root;

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private final org.secretflow.dataproxy.plugin.database.reader.DatabaseRecordReader reader;

    private Future<?> readFuture;

    private final AtomicBoolean hasNext = new AtomicBoolean(true);

    public DatabaseDoGetTaskContext(TaskConfig taskConfig, VectorSchemaRoot root) {
        this.root = root;
        this.taskConfig = taskConfig;
        this.sender = getSender();
        this.reader = new DatabaseRecordReader(taskConfig, sender, taskConfig.getContext().getResultSet());
    }

    public void start() {
        readFuture = executorService.submit(() -> {
            log.info("cscds");
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
    @Override
    public void close() throws Exception {
        this.cancel();
        executorService.shutdown();
    }

    public void putNextPatchData() {
        if ((!hasNext() && readFuture.isDone()) || readFuture.isCancelled() ) {
            return;
        }
        sender.send();
    }

    public boolean hasNext() {
        return hasNext.get();
    }

    private Sender<Record> getSender() {
        int estimatedRecordCount = 1_000;
        return new DatabaseRecordSender(
                estimatedRecordCount,
                new LinkedBlockingQueue<>(estimatedRecordCount),
                root,
                taskConfig.getContext().getTableName(),
                taskConfig.getContext().getDatabaseMetaData(),
                taskConfig.getContext().getResultSet()
                );
    }
}
