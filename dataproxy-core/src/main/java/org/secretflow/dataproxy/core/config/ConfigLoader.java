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

package org.secretflow.dataproxy.core.config;

import java.util.Properties;

/**
 * @author yuexie
 * @date 2024/11/28 16:40
 **/
public interface ConfigLoader {

    /**
     * load properties<br>
     * Read the information and load it into the properties passed in<br>
     * The reading order is sorted by {@link #getPriority()}<br>
     *
     * @param properties properties
     */
    void loadProperties(Properties properties);

    /**
     * get priority:<br>
     * 0: default priority<br>
     * 1: highest priority<br>
     * 2: second highest priority<br>
     * ...<br>
     * 999: lowest priority<br>
     * The higher the value, the higher the priority
     *
     * @return priority
     */
    default int getPriority() {
        return 0;
    }
}
