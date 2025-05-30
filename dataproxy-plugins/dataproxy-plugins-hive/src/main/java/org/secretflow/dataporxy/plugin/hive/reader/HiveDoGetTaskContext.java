package org.secretflow.dataporxy.plugin.hive.reader;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.secretflow.dataporxy.plugin.hive.config.TaskConfig;
import org.secretflow.dataproxy.core.reader.Sender;

import java.lang.reflect.Executable;
import java.sql.ResultSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class HiveDoGetTaskContext implements AutoCloseable{
    private final ResultSet;

    private final Sender<ResultSet> sender;

    private final VectorSchemaRoot root;

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    private Future<?> readFuture;

    private final AtomicBoolean hasNext = new AtomicBoolean(true);

    public HiveDoGetTaskContext(TaskConfig taskConfig, VectorSchemaRoot root) {
        this.root = root;
        this.sender = getSender();
        this.reader = new HiveReader();
    }
    @Override
    public void close() throws Exception {
        this.cancel();
        executorService.shutdown();
    }

    private Sender<Record> getSender() {
        int estimatedRecordCount = 1_000;
        return new HiveRecordSender(
                estimatedRecordCount,
                new LinkedBlockingQueue<>(estimatedRecordCount),
                root,
                "",
                );
    }
}
