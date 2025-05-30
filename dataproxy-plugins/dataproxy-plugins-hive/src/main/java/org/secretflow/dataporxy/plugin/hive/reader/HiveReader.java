package org.secretflow.dataporxy.plugin.hive.reader;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

@Setter
@Slf4j
public class HiveReader extends ArrowReader {

    public HiveReader(BufferAllocator allocator) {
        super(allocator);
    }

    @Override
    public boolean loadNextBatch() throws IOException {

        // TODO: check taskconfig


        return false;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {

    }

    @Override
    protected Schema readSchema() throws IOException {
        return null;
    }

}
