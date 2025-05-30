package org.secretflow.dataporxy.plugin.hive.config;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.secretflow.dataproxy.common.serializer.SensitiveDataSerializer;

public record HiveConnectConfig(@JsonSerialize(using = SensitiveDataSerializer.class) String username,
                                @JsonSerialize(using = SensitiveDataSerializer.class) String password,
                                String endpoint, String database) {
}
