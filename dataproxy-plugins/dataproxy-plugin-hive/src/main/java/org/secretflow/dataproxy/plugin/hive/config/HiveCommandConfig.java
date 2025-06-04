package org.secretflow.dataproxy.plugin.hive.config;

import lombok.Getter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.hive.constant.HiveTypeEnum;

@Getter
public abstract class HiveCommandConfig<T> {
    protected final HiveConnectConfig hiveConnectConfig;
    protected final HiveTypeEnum hiveTypeEnum;
    protected final T commandConfig;

    public HiveCommandConfig(HiveConnectConfig hiveConnectConfig, HiveTypeEnum hiveTypeEnum, T commandConfig) {
        this.hiveConnectConfig = hiveConnectConfig;
        this.hiveTypeEnum = hiveTypeEnum;
        this.commandConfig = commandConfig;
    }
    /*
     * TODO:
     */
    public abstract String taskRunSQL();
    /*
     * TODO
     */
    public abstract Schema getResultSchema();
}
