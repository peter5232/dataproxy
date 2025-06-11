package org.secretflow.dataproxy.plugin.database.reader;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.database.config.TaskConfig;

import java.io.IOException;

@Setter
@Slf4j
public class DatabaseReader extends ArrowReader {
    private DatabaseDoGetTaskContext dbDoGetTaskContext = null;
    private final TaskConfig taskConfig;

    public DatabaseReader(BufferAllocator allocator, TaskConfig taskConfig) {
        super(allocator);
        this.taskConfig = taskConfig;
    }

    @Override
    public boolean loadNextBatch() throws IOException {

        if(dbDoGetTaskContext == null) {
            prepare();
        }

        if(dbDoGetTaskContext.hasNext()) {
            dbDoGetTaskContext.putNextPatchData();
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
            if (dbDoGetTaskContext != null) {
                dbDoGetTaskContext.close();
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
        dbDoGetTaskContext = new DatabaseDoGetTaskContext(this.taskConfig, this.getVectorSchemaRoot());
        dbDoGetTaskContext.start();
    }
}
