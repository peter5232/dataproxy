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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * @author yuexie
 * @date 2024/10/31 19:57
 **/
@Getter
@Slf4j
public class FlightServerContext {

    private final FlightServerConfig flightServerConfig;

    private static final Properties CONFIG_PROPERTIES = new Properties();

    private static class SingletonHolder {
        public static final FlightServerContext INSTANCE = new FlightServerContext();
    }

    private FlightServerContext() {
        init();
        flightServerConfig = new FlightServerConfig(get(FlightServerConfigKey.HOST, String.class), get(FlightServerConfigKey.PORT, Integer.class));
    }

    private void init() {
        ServiceLoader<ConfigLoader> serviceLoader = ServiceLoader.load(ConfigLoader.class);

        serviceLoader.stream()
                .map(ServiceLoader.Provider::get)
                .sorted(Comparator.comparingInt(ConfigLoader::getPriority))
                .forEach(configLoader -> configLoader.loadProperties(CONFIG_PROPERTIES));

        CONFIG_PROPERTIES.forEach((k, v) -> log.info("load config: {}={}", k, v));
    }

    public static FlightServerContext getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public static <T> T getOrDefault(String key, Class<T> tClass, T defaultValue) {
        return Optional.ofNullable(CONFIG_PROPERTIES.get(key)).map(tClass::cast).orElse(defaultValue);
    }

    public static <T> T get(String key, Class<T> tClass) {
        return Optional.ofNullable(CONFIG_PROPERTIES.get(key)).map(tClass::cast).orElse(null);
    }
}
