/*
 * Copyright 2024 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.plugin.odps.converter;

import org.secretflow.dataproxy.core.converter.DataProxyParamConverter;
import org.secretflow.dataproxy.plugin.odps.config.OdpsConnectConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsTableConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsTableQueryConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsWriteConfig;
import org.secretflow.dataproxy.plugin.odps.config.ScqlCommandJobConfig;
import org.secretflow.dataproxy.plugin.odps.constant.OdpsTypeEnum;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

/**
 * @author yuexie
 * @date 2024/10/31 19:40
 **/
public class OdpsParamConverter implements DataProxyParamConverter<ScqlCommandJobConfig, OdpsTableQueryConfig, OdpsWriteConfig> {

    @Override
    public ScqlCommandJobConfig convert(Flightinner.CommandDataMeshSqlQuery request) {
        Domaindatasource.OdpsDataSourceInfo odps = request.getDatasource().getInfo().getOdps();
        return new ScqlCommandJobConfig(convert(odps), request.getQuery().getSql());
    }

    @Override
    public OdpsTableQueryConfig convert(Flightinner.CommandDataMeshQuery request) {

        Domaindatasource.OdpsDataSourceInfo odps = request.getDatasource().getInfo().getOdps();

        Domaindata.DomainData domaindata = request.getDomaindata();

        String tableName = domaindata.getRelativeUri();
        String partitionSpec = request.getQuery().getPartitionSpec();
        OdpsTableConfig odpsTableConfig = new OdpsTableConfig(tableName, partitionSpec, domaindata.getColumnsList());

        if (Flightdm.ContentType.RAW.equals(request.getQuery().getContentType())) {
            return new OdpsTableQueryConfig(convert(odps), OdpsTypeEnum.FILE, odpsTableConfig);
        }

        return new OdpsTableQueryConfig(convert(odps), odpsTableConfig);
    }

    @Override
    public OdpsWriteConfig convert(Flightinner.CommandDataMeshUpdate request) {
        Domaindatasource.OdpsDataSourceInfo odps = request.getDatasource().getInfo().getOdps();
        Domaindata.DomainData domaindata = request.getDomaindata();

        String tableName = domaindata.getRelativeUri();
        String partitionSpec = request.getUpdate().getPartitionSpec();
        OdpsTableConfig odpsTableConfig = new OdpsTableConfig(tableName, partitionSpec, domaindata.getColumnsList());

        if (Flightdm.ContentType.RAW.equals(request.getUpdate().getContentType())) {
            return new OdpsWriteConfig(convert(odps), OdpsTypeEnum.FILE, odpsTableConfig);
        }
        return new OdpsWriteConfig(convert(odps), odpsTableConfig);
    }

    private static OdpsConnectConfig convert(Domaindatasource.OdpsDataSourceInfo odps) {
        return new OdpsConnectConfig(odps.getAccessKeyId(), odps.getAccessKeySecret(), odps.getEndpoint(), odps.getProject());
    }

}
