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

import org.secretflow.dataproxy.core.config.ConfigLoader;

import java.util.Properties;

/**
 * @author yuexie
 * @date 2024/11/28 15:16
 **/
public class DefaultOdpsFlightConfigLoader implements ConfigLoader {

    /**
     * load properties<br>
     * Read the information and load it into the properties passed in<br>
     * The reading order is sorted by {@link #getPriority()}<br>
     *
     * @param properties properties
     */
    @Override
    public void loadProperties(Properties properties) {
        properties.put(OdpsConfigConstant.ConfigKey.MAX_FLIGHT_ENDPOINT, 1);
        properties.put(OdpsConfigConstant.ConfigKey.FLIGHT_ENDPOINT_UPGRADE_TO_MULTI_BATCH_THRESHOLD, 1000_000L);
    }

    @Override
    public int getPriority() {
        return 1;
    }
}
