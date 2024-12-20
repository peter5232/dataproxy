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

package org.secretflow.dataproxy.core.param;

/**
 * @author yuexie
 * @date 2024/10/31 15:59
 **/
public record ParamWrapper(String producerKey, Object param) {

    /**
     * of param
     *
     * @param param param
     * @return param wrapper
     */
    public static ParamWrapper of(String producerKey, Object param) {
        if (param == null) {
            throw new IllegalArgumentException("param is null");
        }
        return new ParamWrapper(producerKey, param);
    }

    /**
     * unwrap param
     *
     * @param distClas dist class
     * @param <D>      dist class type
     * @return dist class
     */
    public <D> D unwrap(Class<D> distClas) {
        if (param == null) {
            throw new IllegalArgumentException("param is null");
        }

        if (!distClas.isAssignableFrom(param.getClass())) {
            throw new IllegalArgumentException("param is not assignable from " + distClas.getName());
        }
        return distClas.cast(param);
    }
}
