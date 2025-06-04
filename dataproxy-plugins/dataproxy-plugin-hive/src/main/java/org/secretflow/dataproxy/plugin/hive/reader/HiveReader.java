package org.secretflow.dataproxy.plugin.hive.reader;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.hive.config.TaskConfig;

import java.io.IOException;

@Setter
@Slf4j
public class HiveReader extends ArrowReader {
    private HiveDoGetTaskContext hiveDoGetTaskContext = null;
    private final TaskConfig taskConfig;

    public HiveReader(BufferAllocator allocator, TaskConfig taskConfig) {
        super(allocator);
        this.taskConfig = taskConfig;
    }

    @Override
    public boolean loadNextBatch() throws IOException {

        if(hiveDoGetTaskContext == null) {
            prepare();
        }

        if(hiveDoGetTaskContext.hasNext()) {
            hiveDoGetTaskContext.putNextPatchData();
            return true;
        }
        return false;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
        try {
            if (hiveDoGetTaskContext != null) {
                hiveDoGetTaskContext.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Schema readSchema() throws IOException {
        return taskConfig.getContext().getSchema();
    }

    public void prepare() throws IOException {
        hiveDoGetTaskContext = new HiveDoGetTaskContext(this.taskConfig, this.getVectorSchemaRoot());
        hiveDoGetTaskContext.start();
    }
}
