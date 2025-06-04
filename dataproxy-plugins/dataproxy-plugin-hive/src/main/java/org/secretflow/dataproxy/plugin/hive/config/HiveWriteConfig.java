package org.secretflow.dataproxy.plugin.hive.config;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.hive.constant.HiveTypeEnum;
import org.secretflow.dataproxy.common.utils.ArrowUtil;

import java.util.stream.Collectors;

public class HiveWriteConfig extends HiveCommandConfig<HiveTableConfig> {
    public HiveWriteConfig(HiveConnectConfig hiveConnectConfig, HiveTableConfig readConfig) {
        super(hiveConnectConfig, HiveTypeEnum.TABLE, readConfig);
    }

    public HiveWriteConfig(HiveConnectConfig hiveConnectConfig, HiveTypeEnum typeEnum, HiveTableConfig readConfig) {
        super(hiveConnectConfig, typeEnum, readConfig);
    }

    @Override
    public String taskRunSQL() {
        return "";
    }

    @Override
    public Schema getResultSchema() {
        return new Schema(commandConfig.columns().stream()
                .map(column ->
                        Field.nullable(column.getName(), ArrowUtil.parseKusciaColumnType(column.getType())))
                .collect(Collectors.toList()));
    }
}
