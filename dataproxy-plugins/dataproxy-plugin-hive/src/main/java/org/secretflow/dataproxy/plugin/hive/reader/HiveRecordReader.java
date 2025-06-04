package org.secretflow.dataproxy.plugin.hive.reader;

import org.secretflow.dataproxy.plugin.hive.config.TaskConfig;
import org.secretflow.dataproxy.plugin.hive.utils.Record;
import org.secretflow.dataproxy.core.reader.AbstractReader;
import org.secretflow.dataproxy.core.reader.Sender;

import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveRecordReader extends AbstractReader<TaskConfig, Record> {

    private final ResultSet resultSet;
    public HiveRecordReader(TaskConfig param, Sender<Record> sender, ResultSet rs){
        super(param, sender);
        this.resultSet = rs;
    }

    @Override
    protected void read(TaskConfig param) {
        int recordCount = 0;
        Record record = null;
        try {
            while(resultSet.next()) {
                record = new Record(resultSet);
                recordCount++;
                this.put(record);
            }

            // 最后一个放入空的record设置last的tag，
            record = new Record();
            record.setLast(true);
            this.put(record);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
