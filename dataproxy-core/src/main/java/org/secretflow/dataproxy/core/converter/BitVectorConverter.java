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

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;
import org.secretflow.dataproxy.core.visitor.ValueVisitor;

/**
 * @author yuexie
 * @date 2024/11/1 16:14
 **/
public class BitVectorConverter extends AbstractValueConverter<Boolean> {


    public BitVectorConverter(ValueVisitor<Boolean> visitor) {
        super(visitor);
    }

    @Override
    public void convertAndSet(ValueVector vector, int index, Object value) {

        if (vector instanceof BitVector bitVector) {
            bitVector.setSafe(index, this.visit(value) ? 1 : 0);
        } else {
            throw new IllegalArgumentException("BitVectorConsumer unsupported vector type: " + vector.getClass().getName());
        }
    }
}
