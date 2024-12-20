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

/**
 * @author yuexie
 * @date 2024/11/1 10:56
 **/
public abstract class AbstractReader<T, R> implements Reader {

    private final T param;
    private final Sender<R> sender;

    public AbstractReader(T param, Sender<R> sender) {
        this.param = param;
        this.sender = sender;
    }

    protected abstract void read(T param);

    @Override
    public void read() {
        this.read(param);
    }

    public void put(R record) throws Exception {
        sender.put(record);
    }

}
