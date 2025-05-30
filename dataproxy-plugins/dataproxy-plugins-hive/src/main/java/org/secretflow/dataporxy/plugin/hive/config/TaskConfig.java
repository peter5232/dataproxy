package org.secretflow.dataporxy.plugin.hive.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

@Getter
@ToString
public class TaskConfig {

    private final long startIndex;
    private final long count;

    private final String tableName;
    private final DatabaseMetaData metaData;
    private final ResultSet resultSet;

    @Setter
    private long currentIndex;

    private final boolean compress;

    @Getter
    @Setter
    private Throwable error;

    public TaskConfig(long startIndex, long count, String tableName, DatabaseMetaData metaData, ResultSet resultSet) {
        this(startIndex, count, true, tableName, metaData, resultSet);
    }

    public TaskConfig(long startIndex, long count, boolean compress, String tableName, DatabaseMetaData metaData, ResultSet resultSet) {
        this.startIndex = startIndex;
        this.count = count;
        this.compress = compress;
        this.currentIndex = startIndex;
        this.tableName = tableName;
        this.metaData = metaData;
        this.resultSet = resultSet;
    }
}
