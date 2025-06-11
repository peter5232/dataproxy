package org.secretflow.dataproxy.plugin.database.config;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.secretflow.dataproxy.common.serializer.SensitiveDataSerializer;

public record DatabaseConnectConfig(@JsonSerialize(using = SensitiveDataSerializer.class) String username,
                                    @JsonSerialize(using = SensitiveDataSerializer.class) String password,
                                    String endpoint, String database) {
}
