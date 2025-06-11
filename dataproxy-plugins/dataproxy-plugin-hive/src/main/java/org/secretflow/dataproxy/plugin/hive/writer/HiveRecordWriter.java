package org.secretflow.dataproxy.plugin.hive.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.hive.config.HiveCommandConfig;
import org.secretflow.dataproxy.plugin.hive.config.HiveConnectConfig;
import org.secretflow.dataproxy.plugin.hive.config.HiveTableConfig;
import org.secretflow.dataproxy.plugin.hive.config.HiveWriteConfig;
import org.secretflow.dataproxy.plugin.hive.utils.HiveUtil;
import org.secretflow.dataproxy.plugin.hive.utils.Record;
import org.secretflow.dataproxy.core.writer.Writer;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class HiveRecordWriter implements Writer {
    private final HiveCommandConfig<?> commandConfig;

    private final HiveConnectConfig hiveConnectConfig;
    private final HiveTableConfig hiveTableConfig;

    private Connection hive;

    public HiveRecordWriter(HiveWriteConfig commandConfig) throws SQLException {
        this.commandConfig = commandConfig;
        this.hiveConnectConfig = commandConfig.getHiveConnectConfig();
        this.hiveTableConfig = commandConfig.getCommandConfig();
        this.prepare();
    }

    private Connection initHiveClient(HiveConnectConfig hiveConnectConfig) throws SQLException {
        if(hiveConnectConfig == null) {
            throw new IllegalArgumentException("connConfig is null");
        }
        return HiveUtil.initHive(hiveConnectConfig);
    }
    private void prepare() throws SQLException {

        hive = initHiveClient(hiveConnectConfig);

        preProcessing(hive, hiveTableConfig.tableName());

    }

    /**
     * 获取字段数据
     *
     * @param fieldVector field vector
     * @param index       index
     * @return value
     */
    private Object getValue(FieldVector fieldVector, int index) {
        if (fieldVector == null || index < 0 || fieldVector.getObject(index) == null) {
            return null;
        }
        ArrowType.ArrowTypeID arrowTypeID = fieldVector.getField().getType().getTypeID();

        switch (arrowTypeID) {
            case Int -> {
                if (fieldVector instanceof IntVector || fieldVector instanceof BigIntVector || fieldVector instanceof SmallIntVector || fieldVector instanceof TinyIntVector) {
                    return fieldVector.getObject(index);
                }
                log.warn("Type INT is not IntVector or BigIntVector or SmallIntVector or TinyIntVector, value is: {}", fieldVector.getObject(index).toString());
            }
            case FloatingPoint -> {
                if (fieldVector instanceof Float4Vector | fieldVector instanceof Float8Vector) {
                    return fieldVector.getObject(index);
                }
                log.warn("Type FloatingPoint is not Float4Vector or Float8Vector, value is: {}", fieldVector.getObject(index).toString());
            }
            case Utf8 -> {
                if (fieldVector instanceof VarCharVector vector) {
                    return new String(vector.get(index), StandardCharsets.UTF_8);
                }
                log.warn("Type Utf8 is not VarCharVector, value is: {}", fieldVector.getObject(index).toString());
            }
            case Null -> {
                return null;
            }
            case Bool -> {
                if (fieldVector instanceof BitVector vector) {
                    return vector.get(index) == 1;
                }
                log.warn("Type BOOL is not BitVector, value is: {}", fieldVector.getObject(index).toString());
            }
            default -> {
                log.warn("Not implemented type: {}, will use default function", arrowTypeID);
                return fieldVector.getObject(index);
            }

        }
        return null;
    }

    @Override
    public void write(VectorSchemaRoot root) {
        final int batchSize = root.getRowCount();
        log.info("hive writer batchSize: {}", batchSize);
        int columnCount = root.getFieldVectors().size();

        String columnName;
        Record record = new Record();
        for(int rowIndex = 0; rowIndex < batchSize; rowIndex ++) {
            for(int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                log.debug("column: {}, type: {}", columnIndex, root.getFieldVectors().get(columnIndex));
                columnName = root.getVector(columnIndex).getField().getName().toLowerCase();

                record.set(columnName, this.getValue(root.getFieldVectors().get(columnIndex), rowIndex));
            }
            try{
                this.insertData(hive, commandConfig.getResultSchema(), hiveTableConfig.tableName(), record.getData());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            log.debug("record: {}", record);
        }
    }

    @Override
    public void flush() {

    }

    // 根据Arrow字段类型转换为JDBC类型
    private static String getJdbcType(Field field) {
        switch (field.getFieldType().getType().getTypeID()) {
            case Int:
                return "INT";
            case Utf8:
                return "VARCHAR(255)";
            case FloatingPoint:
                return "FLOAT";
            // 添加其他类型映射
            default:
                return "VARCHAR(255)"; // 默认使用VARCHAR
        }
    }

    private void createTableFromSchema(Connection hive,Schema schema, String tableName) throws SQLException {

        StringBuilder createTableSql = new StringBuilder("CREATE TABLE "+ tableName + " (");
        for (Field field : schema.getFields()) {
            createTableSql.append("\n   ");
            createTableSql.append(field.getName());
            createTableSql.append(" ");
            createTableSql.append(getJdbcType(field));
            createTableSql.append(",");
        }
        createTableSql.setCharAt(createTableSql.length() - 1, ')');
        Statement stmt = hive.createStatement();
        stmt.executeUpdate(createTableSql.toString());
    }

    // 假设传入的 Arrow Schema 和列名对应的 Object 数据
    public void insertData(Connection conn, Schema arrowSchema, String tableName, Map<String, Object> data) throws SQLException {
        // 构建 SQL 插入语句
        StringBuilder sql = new StringBuilder("INSERT INTO "+ tableName +" (");
        StringBuilder values = new StringBuilder("VALUES (");

        // 获取 Schema 中的所有列
        List<Field> fields = arrowSchema.getFields();

        // 构建列名部分和 VALUES 部分
        List<Object> valueList = new ArrayList<>();
        for (Field field : fields) {
            String columnName = field.getName();
            sql.append(columnName).append(", ");

            // 获取该列的值
            Object value = data.get(columnName);
            if (value == null) {
                values.append("NULL, ");
            } else {
                values.append("?, ");
                valueList.add(value);  // 将数据值添加到 valueList
            }
        }

        // 去掉最后的 ", " 并关闭括号
        sql.setLength(sql.length() - 2);
        sql.append(") ");

        values.setLength(values.length() - 2);
        values.append(")");

        // 完成 SQL 插入语句
        sql.append(values);

        // 执行插入操作
        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            // 设置参数
            int index = 1;
            for (Object value : valueList) {
                setStatementParameter(stmt, index++, value);
            }

            stmt.executeUpdate();
        }
    }

    // 根据不同的列类型，将 Object 转换为合适的 JDBC 数据类型
    private static void setStatementParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.NULL);
        } else if (value instanceof Integer) {
            stmt.setInt(index, (Integer) value);
        } else if (value instanceof String) {
            stmt.setString(index, (String) value);
        } else if (value instanceof Long) {
            stmt.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            stmt.setDouble(index, (Double) value);
        } else if (value instanceof Float) {
            stmt.setFloat(index, (Float) value);
        } else if (value instanceof Boolean) {
            stmt.setBoolean(index, (Boolean) value);
        } else {
            stmt.setObject(index, value);  // 默认使用 setObject
        }
    }
    /// 如果表格不存在，创建表格
    private void preProcessing(Connection hive, String tableName) throws SQLException {
        if(!isExistsTable(hive, tableName)) {
            createTableFromSchema(hive, commandConfig.getResultSchema(), tableName);
            log.info("hive table is not exists, create table successful, table name: {}", tableName);
        } else {
            log.info("hive table is exists, table name: {}", tableName);
        }
    }



    private boolean isExistsTable(Connection hive, String tableName) throws SQLException {
        DatabaseMetaData metaData = hive.getMetaData();
        ResultSet resultSet = metaData.getTables(null, null, tableName, new String[]{"TABLE"}); {
            if(resultSet.next()) {
                return true;
            } else {
                return false;
            }
        }
    }
    void setRecordValue(Record record, int columnIndex, Object value) {
        if(value == null) {
            record.set(columnIndex, null);
            log.warn("table name: {} record set null value. index: {}", hiveTableConfig.tableName(), columnIndex);
            return;
        }

        int hiveType = record.getColumnType(columnIndex);
        log.debug("record hive type: {}", hiveType);
        record.set(columnIndex, value);
    }
}
