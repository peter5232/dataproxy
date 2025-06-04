package org.secretflow.dataproxy.plugin.hive.config;

import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.hive.constant.HiveTypeEnum;

public class ScqlCommandJobConfig extends HiveCommandConfig<String> {
    public ScqlCommandJobConfig(HiveConnectConfig hiveConnectConfig, String querySql) {
        super(hiveConnectConfig, HiveTypeEnum.SQL, querySql);
    }

    @Override
    public String taskRunSQL(){
        return commandConfig;
    }

    @Override
    public Schema getResultSchema() {
        return null;
    }
}
