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

package org.secretflow.dataproxy.core.converter;

import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.secretflow.dataproxy.core.visitor.ValueVisitor;

/**
 * @author yuexie
 * @date 2024/12/4 16:32
 **/
public class TimeStampNanoVectorConverter extends AbstractValueConverter<Long> {
    public TimeStampNanoVectorConverter(ValueVisitor<Long> visitor) {
        super(visitor);
    }

    @Override
    public void convertAndSet(ValueVector vector, int index, Object value) {
        if (vector instanceof TimeStampMilliVector timeStampMilliVector) {
            timeStampMilliVector.set(index, this.visit(value));
        } else {
            throw new IllegalArgumentException("TimeStampNanoVectorConverter unsupported vector type: " + vector.getClass().getName());
        }
    }
}
