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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.odps.config.OdpsConnectConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsTableConfig;
import org.secretflow.dataproxy.plugin.odps.utils.OdpsUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

/**
 * @author yuexie
 * @date 2024/11/11 16:00
 **/
public class OdpsResourceReader extends ArrowReader {

    private static final String FIELD_NAME = "binary_data";
    private static final int BATCH_SIZE = 1024 * 1024;
    private static final int BYTES_READ = 1024;
    private static final int BYTES_THRESHOLD = BATCH_SIZE - BYTES_READ;

    private final OdpsConnectConfig connectConfig;

    private final OdpsTableConfig tableInfo;

    private InputStream inputStream;
    public OdpsResourceReader(BufferAllocator allocator, OdpsConnectConfig connectConfig, OdpsTableConfig tableInfo) {
        super(allocator);
        this.connectConfig = connectConfig;
        this.tableInfo = tableInfo;

        this.prepare();
    }

    /**
     * Load the next ArrowRecordBatch to the vector schema root if available.
     *
     * @return true if a batch was read, false on EOS
     * @throws IOException on error
     */
    @Override
    public boolean loadNextBatch() throws IOException {
        VectorSchemaRoot root = getVectorSchemaRoot();

        VarBinaryVector vector = (VarBinaryVector) root.getVector(FIELD_NAME);
        vector.allocateNew(1);

        if (vector.getDataBuffer().capacity() < BATCH_SIZE) {
            vector.reallocDataBuffer(BATCH_SIZE);
        }

        ArrowBuf dataBuffer = vector.getDataBuffer();

        int l = readRangeToBuffer(dataBuffer);
        if (l == 0) {
            return false;
        }

        vector.getOffsetBuffer().setInt(VarBinaryVector.OFFSET_WIDTH, l);
        BitVectorHelper.setBit(vector.getValidityBuffer(), 0);
        vector.setLastSet(0);

        root.setRowCount(1);

        return true;
    }

    /**
     * Return the number of bytes read from the ReadChannel.
     *
     * @return number of bytes read
     */
    @Override
    public long bytesRead() {
        try {
            if (inputStream != null) {
                return inputStream.available();
            }
            throw DataproxyException.of(DataproxyErrorCode.FILE_READ_STREAM_CREATE_FAILED);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Close the underlying read source.
     *
     * @throws IOException on error
     */
    @Override
    protected void closeReadSource() throws IOException {

        if (Objects.nonNull(inputStream)) {
            inputStream.close();
        }
    }

    /**
     * Read the Schema from the source, will be invoked at the beginning the initialization.
     *
     * @return the read Schema
     * @throws IOException on error
     */
    @Override
    protected Schema readSchema() throws IOException {
        return new Schema(List.of(Field.notNullable(FIELD_NAME, new ArrowType.Binary())));
    }

    private void prepare() {
        Odps odps = OdpsUtil.initOdps(connectConfig);
        try {
            inputStream = odps.resources().getResourceAsStream(tableInfo.tableName());
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
    }

    private int readRangeToBuffer(ArrowBuf valueBuffer) {
        if (inputStream == null) {
            return 0;
        }

        try {

            byte[] bytes = new byte[BYTES_READ];
            int length;
            int totalBytesRead = 0;
            while ((length = inputStream.read(bytes)) != -1) {
                valueBuffer.writeBytes(bytes, 0, length);
                totalBytesRead += length;
                if (totalBytesRead >= BYTES_THRESHOLD) {
                    break;
                }
            }
            return totalBytesRead;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
