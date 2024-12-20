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

import lombok.extern.slf4j.Slf4j;
import org.secretflow.dataproxy.common.utils.EnvVarUtils;
import org.secretflow.dataproxy.core.config.ConfigLoader;

import java.util.Optional;
import java.util.Properties;

/**
 * @author yuexie
 * @date 2024/12/6 15:45
 **/
@Slf4j
public class EnvironmentOdpsFlightConfigLoader implements ConfigLoader {

    /**
     * load properties<br>
     * Read the information and load it into the properties passed in<br>
     * The reading order is sorted by {@link #getPriority()}<br>
     *
     * @param properties properties
     */
    @Override
    public void loadProperties(Properties properties) {

        try {
            log.info("Load odps flight config from system env.");
            Optional<Integer> maxEndpoint = EnvVarUtils.getInt(OdpsConfigConstant.ConfigKey.MAX_FLIGHT_ENDPOINT);
            if (maxEndpoint.isPresent()) {
                log.debug("Load odps flight config `MAX_FLIGHT_ENDPOINT` from system env, limits range 1 to 5. key: {}, value: {}", OdpsConfigConstant.ConfigKey.MAX_FLIGHT_ENDPOINT, maxEndpoint.get());
                properties.put(OdpsConfigConstant.ConfigKey.MAX_FLIGHT_ENDPOINT, EnvVarUtils.getEffectiveValue(maxEndpoint.get(), 1, 5));
            }

            Optional<Long> batchThreshold = EnvVarUtils.getLong(OdpsConfigConstant.ConfigKey.FLIGHT_ENDPOINT_UPGRADE_TO_MULTI_BATCH_THRESHOLD);
            if (batchThreshold.isPresent()) {
                log.debug("Load odps flight config `MAX_FLIGHT_ENDPOINT` from system env, limits range 300,000 to 100,000,000. key: {}, value: {}", OdpsConfigConstant.ConfigKey.FLIGHT_ENDPOINT_UPGRADE_TO_MULTI_BATCH_THRESHOLD, batchThreshold.get());
                properties.put(OdpsConfigConstant.ConfigKey.FLIGHT_ENDPOINT_UPGRADE_TO_MULTI_BATCH_THRESHOLD, EnvVarUtils.getEffectiveValue(batchThreshold.get(), 300_000L, 100_000_000L));
            }
        } catch (Exception e) {
            log.error("Failed to load odps flight config from system env. This error will be ignored and some configurations will not take effect. error: {}", e.getMessage(), e);
        }

    }

    @Override
    public int getPriority() {
        return 3;
    }


}
