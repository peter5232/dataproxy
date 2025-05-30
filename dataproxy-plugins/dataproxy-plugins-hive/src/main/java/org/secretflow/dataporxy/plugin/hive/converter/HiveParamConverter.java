package org.secretflow.dataporxy.plugin.hive.converter;

import org.apache.arrow.flight.NoOpFlightProducer;
import org.secretflow.dataporxy.plugin.hive.config.*;
import org.secretflow.dataproxy.core.converter.DataProxyParamConverter;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

public class HiveParamConverter implements DataProxyParamConverter<ScqlCommandJobConfig, HiveTableQueryConfig, HiveWriteConfig> {

    @Override
    public ScqlCommandJobConfig convert(Flightinner.CommandDataMeshSqlQuery request) {
        Domaindatasource.DatabaseDataSourceInfo hive = request.getDatasource().getInfo().getDatabase();
        return new ScqlCommandJobConfig(convert(hive), request.getQuery().getSql());
    }

    @Override
    public HiveTableQueryConfig convert(Flightinner.CommandDataMeshQuery request) {
        Domaindatasource.DatabaseDataSourceInfo hive = request.getDatasource().getInfo().getDatabase();
        Domaindata.DomainData domaindata = request.getDomaindata();

        String tableName = domaindata.getRelativeUri();

        String partitionSpec = request.getQuery().getPartitionSpec();

        HiveTableConfig hiveTableConfig = new HiveTableConfig(tableName, partitionSpec, domaindata.getColumnsList());

        return new HiveTableQueryConfig(convert(hive), hiveTableConfig);
    }

    @Override
    public HiveWriteConfig convert(Flightinner.CommandDataMeshUpdate request) {
        Domaindatasource.DatabaseDataSourceInfo hive = request.getDatasource().getInfo().getDatabase();
        Domaindata.DomainData domainData = request.getDomaindata();

        String tableName = domainData.getRelativeUri();

        String partitionSpec = request.getUpdate().getPartitionSpec();
        HiveTableConfig hivetableConfig = new HiveTableConfig(tableName, partitionSpec, domainData.getColumnsList());
        return new HiveWriteConfig(convert(hive), hivetableConfig);

    }


    private static HiveConnectConfig convert(Domaindatasource.DatabaseDataSourceInfo hive) {
        return new HiveConnectConfig(hive.getUser(), hive.getPassword(), hive.getEndpoint(), hive.getDatabase());
    }
}
