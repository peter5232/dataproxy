package org.secretflow.dataproxy.plugin.hive.constant;

import lombok.Getter;

@Getter
public enum HiveTypeEnum {
    TABLE("table"),

    SQL("sql");

    private final String type;

    HiveTypeEnum(String type) {
        this.type = type;
    }
}
