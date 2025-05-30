package org.secretflow.dataporxy.plugin.hive.reader;

import org.secretflow.dataporxy.plugin.hive.config.TaskConfig;
import org.secretflow.dataproxy.core.reader.AbstractReader;
import org.secretflow.dataproxy.core.reader.Sender;

import java.sql.ResultSet;

public class HiveRecordReader extends AbstractReader<TaskConfig, ResultSet> {

    public HiveRecordReader(TaskConfig param, Sender<ResultSet> sender){
        super(param, sender);

    }

    @Override
    protected void read(TaskConfig param) {
        int recordCount = 0;

        while() {

        }
    }

}
