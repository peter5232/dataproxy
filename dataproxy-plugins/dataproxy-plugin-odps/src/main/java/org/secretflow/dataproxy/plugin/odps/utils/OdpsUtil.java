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

package org.secretflow.dataproxy.plugin.odps.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.secretflow.dataproxy.plugin.odps.config.OdpsConnectConfig;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author yuexie
 * @date 2024/11/3 22:30
 **/
public class OdpsUtil {

    public final static boolean OVER_WRITE = true;


    public static Odps initOdps(OdpsConnectConfig config) {

        Account account = new AliyunAccount(config.accessKeyId(), config.accessKeySecret());
        Odps odps = new Odps(account);
        odps.setEndpoint(config.endpoint());
        odps.setDefaultProject(config.projectName());

        return odps;

    }

    public static Map<String, String> getSqlFlag() {
        HashMap<String, String> hints = new LinkedHashMap<>();
        hints.put("odps.sql.type.system.odps2", "true");
        return hints;
    }

    public static Field convertOdpsColumnToArrowField(Column column) {
        return Field.nullable(column.getName(), parseOdpsColumnType(column.getTypeInfo()));
    }

    /**
     * {@see com.aliyun.odps.commons.util.ArrowUtils#getArrowType(com.aliyun.odps.type.TypeInfo)}
     * @param type {@link TypeInfo}
     * @return {@link ArrowType}
     */
    private static ArrowType parseOdpsColumnType(TypeInfo type) {

        OdpsType odpsType = type.getOdpsType();
        return switch (odpsType) {
            case JSON,CHAR,VARCHAR, STRING -> Types.MinorType.VARCHAR.getType();
            case BINARY -> Types.MinorType.VARBINARY.getType();
            case TINYINT -> Types.MinorType.TINYINT.getType();
            case SMALLINT ->Types.MinorType.SMALLINT.getType();
            case INT -> Types.MinorType.INT.getType();
            case BIGINT -> Types.MinorType.BIGINT.getType();
            case BOOLEAN -> Types.MinorType.BIT.getType();
            case FLOAT -> Types.MinorType.FLOAT4.getType();
            case DOUBLE -> Types.MinorType.FLOAT8.getType();
            case DATE -> Types.MinorType.DATEDAY.getType();
            case DATETIME -> Types.MinorType.DATEMILLI.getType();
            //TODO: 8 bytes => 12 bytes
//            case TIMESTAMP, TIMESTAMP_NTZ -> Types.MinorType.TIMESTAMPNANO.getType();
            case TIMESTAMP, TIMESTAMP_NTZ -> Types.MinorType.TIMESTAMPMILLI.getType();
            case ARRAY -> Types.MinorType.LIST.getType();
            case INTERVAL_DAY_TIME -> Types.MinorType.INTERVALDAY.getType();
            case INTERVAL_YEAR_MONTH -> Types.MinorType.INTERVALYEAR.getType();
            case STRUCT -> Types.MinorType.STRUCT.getType();
            case MAP -> Types.MinorType.MAP.getType();
            case DECIMAL -> {
                if (type instanceof DecimalTypeInfo decimalTypeInfo) {
                    yield new ArrowType.Decimal((decimalTypeInfo).getPrecision(), (decimalTypeInfo).getScale(), 128);
                } else {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                }
            }
            default ->
                throw new UnsupportedOperationException("Unsupported type: " + type);
        };
    }
}
