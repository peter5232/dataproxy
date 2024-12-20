/*
 * Copyright 2023 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.integration.tests.config;

import org.secretflow.dataproxy.integration.tests.KusciaConnectorConfig;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;

/**
 * @author muhong
 * @date 2023-11-17 11:08
 */
public class OdpsKusciaConnectorConfig implements KusciaConnectorConfig {

    private final static String ENDPOINT = "";
    private final static String AK = "";
    private final static String SK = "";
    private final static String PROJECT_NAME = "";

    @Override
    public Domaindatasource.DomainDataSource getDatasource() {

        return Domaindatasource.DomainDataSource
                .newBuilder()
                .setDatasourceId("default-datasource")
                .setType("odps")
                .setStatus("Available")
                .setInfo(Domaindatasource.DataSourceInfo.newBuilder()
                        .setOdps(odpsDomainDataSourceInfo())
                        .build())
                .build();
    }

    @Override
    public Domaindata.DomainData getDataset() {
        return odpsDomainDataInfo();
    }

    private static Domaindatasource.OdpsDataSourceInfo odpsDomainDataSourceInfo() {
        return Domaindatasource.OdpsDataSourceInfo.newBuilder()
                .setEndpoint(ENDPOINT)
                .setAccessKeyId(AK)
                .setAccessKeySecret(SK)
                .setProject(PROJECT_NAME)
                .build();
    }

    private static Domaindata.DomainData odpsDomainDataInfo() {
        return Domaindata.DomainData.newBuilder()
                .setType("table")
                .setDatasourceId("default-datasource")
//                            .setRelativeUri("test_user1")
                .setRelativeUri("test_user")
//                            .setRelativeUri("39splitouput5")
//                            .setRelativeUri("alice_table")
                .setFileFormat(Common.FileFormat.CSV)
                .addColumns(Common.DataColumn.newBuilder().setType("str").setName("name").setComment("").build())
//                                    .addColumns(Common.DataColumn.newBuilder().setType("str").setName("id1").setComment("").build())
                .addColumns(Common.DataColumn.newBuilder().setType("int").setName("age").setComment("").build())
                .addColumns(Common.DataColumn.newBuilder().setType("str").setName("job").setComment("").build())
//                                    .addColumns(Common.DataColumn.newBuilder().setType("str").setName("col2_string").setComment("").build())
                .addColumns(Common.DataColumn.newBuilder().setType("bool").setName("boolean_col").setComment("boolean_col").build())
                .build();
    }
}
