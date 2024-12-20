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

package org.secretflow.dataproxy.core.repository;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.secretflow.dataproxy.core.param.ParamWrapper;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author yuexie
 * @date 2024/10/31 16:38
 **/
public final class CaffeineDataRepository implements ParamWrapperRepository {

    private static final int timeout = 300;

    private final Cache<String, ParamWrapper> cache;

    private CaffeineDataRepository() {
        cache = Caffeine.newBuilder()
                .initialCapacity(4)
                .maximumSize(32)
                .expireAfterWrite(timeout, TimeUnit.SECONDS)
                .build();
    }

    private static final class CaffeineDataRepositoryHolder {
        private static final CaffeineDataRepository instance = new CaffeineDataRepository();
    }

    public static CaffeineDataRepository getInstance() {
        return CaffeineDataRepositoryHolder.instance;
    }


    @Override
    public void put(String key, ParamWrapper value) {
        cache.put(key, value);
    }

    @Override
    public Optional<ParamWrapper> getIfPresent(String key) {
        return Optional.ofNullable(cache.getIfPresent(key));
    }

    @Override
    public void remove(String key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidate(String key) {
        cache.invalidate(key);
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }

}
