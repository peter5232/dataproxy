package org.secretflow.dataproxy.plugin.hive.reader;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.hive.config.*;
import org.secretflow.dataproxy.plugin.hive.constant.HiveTypeEnum;
import org.secretflow.dataproxy.plugin.hive.utils.HiveUtil;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.v1alpha1.common.Common;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

@Slf4j
public class HiveDoGetContext {

    private final HiveCommandConfig<?> hiveCommandConfig;

    @Getter
    private long count;

    @Getter
    private Schema schema;

    @Getter
    private ResultSet resultSet;

    @Getter
    private DatabaseMetaData databaseMetaData;

    @Getter
    private String tableName;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public HiveDoGetContext(HiveCommandConfig<?> config) {
        this.hiveCommandConfig = config;
        try {
            prepare();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<TaskConfig> getTaskConfigs() {
        return Collections.singletonList(new TaskConfig(this, 0, count));
    }

    private void prepare() throws SQLException {
        HiveConnectConfig hiveConnectConfig = hiveCommandConfig.getHiveConnectConfig();
        Connection hive = HiveUtil.initHive(hiveConnectConfig);
        String querySql;

        if (hiveCommandConfig instanceof ScqlCommandJobConfig scqlReadJobConfig) {
            querySql = scqlReadJobConfig.getCommandConfig();
        } else if (hiveCommandConfig instanceof HiveTableQueryConfig hiveTableQueryConfig) {
            HiveTableConfig tableConfig = hiveTableQueryConfig.getCommandConfig();
            this.tableName = tableConfig.tableName();
            querySql = this.buildSql(this.tableName, tableConfig.columns().stream().map(Common.DataColumn::getName).toList(), tableConfig.partition());
            this.schema = hiveCommandConfig.getResultSchema();
        } else {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Unsupported read parameter type: " + hiveCommandConfig.getClass());
        }
        this.executeSqlTaskAndHandleResult(hive, this.tableName, querySql);
    }

    private void executeSqlTaskAndHandleResult(Connection connection, String tableName, String querySql) {
        Throwable throwable = null;
        log.info("hive execute sql: {}", querySql);
        try {
            readWriteLock.writeLock().lock();
            this.databaseMetaData = connection.getMetaData();
            Statement stmt = connection.createStatement();
            resultSet = stmt.executeQuery(querySql);
            if (hiveCommandConfig.getHiveTypeEnum() == HiveTypeEnum.SQL) {
                this.initArrowSchemaFromColumns(connection.getMetaData(), tableName);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static ArrowType getArrowType(String jdbcType) {
        switch (jdbcType.toLowerCase()) {
            case "int":
            case "integer":
                return Types.MinorType.INT.getType();
            case "bigint":
                return Types.MinorType.BIGINT.getType();
            case "float":
                return Types.MinorType.FLOAT4.getType();
            case "double":
                return Types.MinorType.FLOAT8.getType();
            case "varchar":
            case "string":
                Types.MinorType.VARCHAR.getType();
            case "boolean":
                Types.MinorType.BIT.getType();
            case "date":
                Types.MinorType.DATEDAY.getType();
            case "timestamp":
                Types.MinorType.TIMESTAMPMILLI.getType();
            default:
                throw new IllegalArgumentException("Unsupported JDBC type: " + jdbcType);
        }
    }

    private void initArrowSchemaFromColumns(DatabaseMetaData metaData, String tableName) throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, tableName, null);
        List<Field> fields = new ArrayList<>();
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String columnType = columns.getString("TYPE_NAME");

            ArrowType arrowType = getArrowType(columnType);
            Field field = new Field(columnName, FieldType.nullable(arrowType), null);
            fields.add(field);
        }
        schema = new Schema(fields);
    }

    private String buildSql(String tableName, List<String> fields, String whereClause) {
        final Pattern columnOrValuePattern = Pattern.compile("^[\\u00b7A-Za-z0-9\\u4e00-\\u9fa5\\-_,.]*$");

        if (!columnOrValuePattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        log.info("whereClause: {}", whereClause);

        return "select " + String.join(",", fields) + " from " + tableName;
    }

}
