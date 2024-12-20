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

package org.secretflow.dataproxy.core.visitor;

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * @author yuexie
 * @date 2024/11/1 20:01
 **/
@Slf4j
public class LongValueVisitor implements ValueVisitor<Long> {

    @Override
    public Long visit(Long value) {
        return value;
    }
    @Override
    public Long visit(String value) {
        return Long.valueOf(value);
    }

    @Override
    public Long visit(Object value) {

        log.debug("type: {}, value: {}",value.getClass().getName(), value);

        if (value instanceof Long longValue) {
            return visit(longValue);
        } else if (value instanceof Date dateValue) {
            return this.visit(dateValue);
        } else if (value instanceof LocalDateTime localDateTime) {
            return this.visit(localDateTime);
        } else if (value instanceof ZonedDateTime zonedDateTime) {
            return this.visit(zonedDateTime);
        } else if (value instanceof LocalDate localDate) {
            return this.visit(localDate);
        } else if (value instanceof Instant instant) {
            return this.visit(instant);
        }

        return visit(value.toString());
    }

    @Override
    public Long visit(Double value) {
        return value.longValue();
    }

    @Override
    public Long visit(Date value) {
        return value.getTime();
    }

    @Override
    public Long visit(boolean value) {
        return value ? 1L : 0L;
    }

    @Override
    public Long visit(Short value) {
        return value.longValue();
    }

    @Override
    public Long visit(Integer value) {
        return value.longValue();
    }

    @Override
    public Long visit(Float value) {
        return value.longValue();
    }

    @Override
    public Long visit(ZonedDateTime value) {
        return value.toInstant().toEpochMilli();
    }

    @Override
    public Long visit(LocalDateTime value) {
        return value.toInstant(ZoneOffset.of(ZoneId.systemDefault().getId())).toEpochMilli();
    }

    @Override
    public Long visit(LocalDate value) {
        return value.toEpochDay();
    }

    @Override
    public Long visit(Instant value) {
        log.debug("visit instant: {}", value.toEpochMilli());
        return value.toEpochMilli();
    }
}
