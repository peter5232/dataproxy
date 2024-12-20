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

package org.secretflow.dataproxy.plugin.odps.config;

import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.odps.constant.OdpsTypeEnum;

/**
 * @author yuexie
 * @date 2024/10/30 17:47
 **/
public class ScqlCommandJobConfig extends OdpsCommandConfig<String> {

    public ScqlCommandJobConfig(OdpsConnectConfig odpsConnectConfig, String querySql) {
        super(odpsConnectConfig, OdpsTypeEnum.SQL, querySql);
    }

    @Override
    public String taskRunSQL() {
        return commandConfig;
    }

    @Override
    public Schema getResultSchema() {
        // Initialize the query and use the table schema returned by the ODPS query
        return null;
    }
}
