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

package org.secretflow.dataproxy.core.spi.producer;


import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;

/**
 * @author yuexie
 * @date 2024/10/30 17:29
 **/
public interface DataProxyFlightProducer extends FlightProducer {


    Schema DEFACT_SCHEMA = new Schema(Collections.emptyList());

    /**
     * Obtain the data type used for registration name and identification processing.
     *
     * @return producer name
     */
    String getProducerName();

}
