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

/**
 * @author yuexie
 * @date 2024/11/28 17:27
 **/
public class OdpsConfigConstant {

    public static class ConfigKey {

        /**
         * The maximum number of flight endpoints that can be split on the server
         */
        public static final String MAX_FLIGHT_ENDPOINT = "FLIGHT_ENDPOINT_ODPS_MAX";

        /**
         * The threshold for upgrading to multi-batch mode
         */
        public static final String FLIGHT_ENDPOINT_UPGRADE_TO_MULTI_BATCH_THRESHOLD = "FLIGHT_ENDPOINT_ODPS_UPGRADE_THRESHOLD";

    }
}
