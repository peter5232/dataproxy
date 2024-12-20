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

package org.secretflow.dataproxy.core.reader;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * @author yuexie
 * @date 2024/10/31 21:12
 **/
public interface ReadJobContext {

    /**
     * split
     * @param num
     * @return
     */
    List<Reader> split(int num);

    boolean hasNext();

    boolean initAndStartSender(VectorSchemaRoot root);

    void waiteFinished();

    Schema getResultSchema();
}
