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

import org.secretflow.dataproxy.core.param.ParamWrapper;

import java.util.Optional;

/**
 * @author yuexie
 * @date 2024/10/31 17:14
 **/
public interface ParamWrapperRepository {

    void put(String key, ParamWrapper value);

    Optional<ParamWrapper> getIfPresent(String key);

    void remove(String key);

    void invalidate(String key);

    void clear();
}
