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

package org.secretflow.dataproxy.plugin.odps.writer;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.core.writer.Writer;
import org.secretflow.dataproxy.plugin.odps.config.OdpsConnectConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsTableConfig;
import org.secretflow.dataproxy.plugin.odps.io.DynamicSequenceInputStream;
import org.secretflow.dataproxy.plugin.odps.utils.OdpsUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yuexie
 * @date 2024/11/11 14:50
 **/
@Slf4j
public class OdpsResourceWriter implements Writer {

    private final OdpsConnectConfig odpsConnectConfig;

    private final OdpsTableConfig odpsTableConfig;

    private Odps odps;

    private static final String FIELD_NAME = "binary_data";

    private final DynamicSequenceInputStream dynamicSequenceInputStream = new DynamicSequenceInputStream();

    private final AtomicBoolean initedFileResource = new AtomicBoolean(false);

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private Future<?> future = null;

    public OdpsResourceWriter(OdpsConnectConfig odpsConnectConfig, OdpsTableConfig odpsTableConfig) {
        this.odpsConnectConfig = odpsConnectConfig;
        this.odpsTableConfig = odpsTableConfig;
        initOdps();
    }


    @Override
    public void write(VectorSchemaRoot root) {

        if (future != null && future.isDone()) {
            throw new RuntimeException("Odps resource writer is closed");
        }

        FieldVector vector = root.getVector(FIELD_NAME);

        if (vector instanceof VarBinaryVector varBinaryVector) {

            int rowCount = root.getRowCount();
            for (int row = 0; row < rowCount; row++) {
                byte[] bytes = varBinaryVector.get(row);

                dynamicSequenceInputStream.appendStream(new ByteArrayInputStream(bytes));

                if (!initedFileResource.get()) {
                    future = executorService.submit(() -> createOrUpdateResource(odps, odpsTableConfig.tableName(), dynamicSequenceInputStream));
                }

            }
        } else {
            throw DataproxyException.of(DataproxyErrorCode.UNSUPPORTED_FIELD_TYPE, "Only support VarBinaryVector type");
        }
    }

    @Override
    public void flush() {
        // no implementation
        dynamicSequenceInputStream.setCompleted();
        try {
            future.get();
            dynamicSequenceInputStream.close();
            executorService.shutdown();
        } catch (IOException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void initOdps() {
        odps = OdpsUtil.initOdps(odpsConnectConfig);
    }

    private static boolean resourceExists(Odps odps, String resourceName) throws OdpsException {
        try {
            Resource resource = odps.resources().get(resourceName);
            resource.reload();
            return true;
        } catch (NoSuchObjectException e) {
            return false;
        }
    }

    private void createOrUpdateResource(Odps odps, String resourceName, InputStream inputStream) {
        try {

            if (initedFileResource.get()) {
                return;
            }
            initedFileResource.set(true);

            FileResource resource = new FileResource();
            resource.setName(resourceName);

            if (resourceExists(odps, resourceName)) {
                odps.resources().update(resource, inputStream);
            } else {
                odps.resources().create(resource, inputStream);
            }

        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
    }
}
