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

package org.secretflow.dataproxy.server;

/**
 * Kuscia接口转换器
 *
 * @author muhong
 * @date 2023-08-30 18:55
 */
public class ProtoObjConvertor {

    /**
     * 将 Kuscia gRPC 的数据源元信息转化为数据源元信息
     */
//    public static Datasource fromProto(Domaindatasource.DomainDataSource domainDataSource) {
//
//        return Datasource.builder()
//            .datasourceId(domainDataSource.getDatasourceId())
//            .name(domainDataSource.getName())
//            .connConfig(fromProto(domainDataSource.getType(), domainDataSource.getInfo()))
//            .writable(true)
//            .build();
//    }
//
//    public static DatasourceConnConfig fromProto(String domainDataSourceType, Domaindatasource.DataSourceInfo dataSourceInfo) {
//        switch (domainDataSourceType) {
//            case "localfs": {
//                LocalFileSystemConnConfig connConfig = LocalFileSystemConnConfig.builder().build();
//                if (dataSourceInfo.hasLocalfs()) {
//                    connConfig.setPath(dataSourceInfo.getLocalfs().getPath());
//                }
//
//                return DatasourceConnConfig.builder()
//                    .type(DatasourceTypeEnum.LOCAL_HOST)
//                    .connConfig(connConfig)
//                    .build();
//            }
//            case "oss": {
//                if (!dataSourceInfo.hasOss()) {
//                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_NOT_EXIST_ERROR, "OSS连接信息缺失");
//                }
//
//                DatasourceTypeEnum type = null;
//                switch (dataSourceInfo.getOss().getStorageType()) {
//                    case "oss":
//                        type = DatasourceTypeEnum.OSS;
//                        break;
//                    case "minio":
//                        type = DatasourceTypeEnum.MINIO;
//                        break;
//                    default:
//                        type = DatasourceTypeEnum.OSS;
//                }
//
//                ObjectFileSystemConnConfig connConfig = ObjectFileSystemConnConfig.builder()
//                    .endpoint(dataSourceInfo.getOss().getEndpoint())
//                    .bucket(dataSourceInfo.getOss().getBucket())
//                    .objectKeyPrefix(dataSourceInfo.getOss().getPrefix())
//                    .accessKey(dataSourceInfo.getOss().getAccessKeyId())
//                    .accessSecret(dataSourceInfo.getOss().getAccessKeySecret())
//                    .build();
//                return DatasourceConnConfig.builder()
//                    .type(type)
//                    .connConfig(connConfig)
//                    .build();
//            }
//            case "mysql": {
//                if (!dataSourceInfo.hasDatabase()) {
//                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_NOT_EXIST_ERROR, "数据库连接信息缺失");
//                }
//
//                MysqlConnConfig connConfig = MysqlConnConfig.builder()
//                    .host(dataSourceInfo.getDatabase().getEndpoint())
//                    .userName(dataSourceInfo.getDatabase().getUser())
//                    .password(dataSourceInfo.getDatabase().getPassword())
//                    .database(dataSourceInfo.getDatabase().getDatabase())
//                    .build();
//                return DatasourceConnConfig.builder()
//                    .type(DatasourceTypeEnum.MYSQL)
//                    .connConfig(connConfig)
//                    .build();
//            }
//            case "odps": {
//                if (!dataSourceInfo.hasOdps()) {
//                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_NOT_EXIST_ERROR, "数据库连接信息缺失");
//                }
//
//                OdpsConnConfig config =
//                        OdpsConnConfig.builder()
//                                .accessKeyId(dataSourceInfo.getOdps().getAccessKeyId())
//                                .accessKeySecret(dataSourceInfo.getOdps().getAccessKeySecret())
//                                .projectName(dataSourceInfo.getOdps().getProject())
//                                .endpoint(dataSourceInfo.getOdps().getEndpoint())
//                                .build();
//
//                return DatasourceConnConfig.builder()
//                    .type(DatasourceTypeEnum.ODPS)
//                    .connConfig(config)
//                    .build();
//            }
//            default:
//                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "不支持的数据源类型 " + domainDataSourceType);
//        }
//    }
//
//    public static Dataset fromProto(Domaindata.DomainData domainData, Datasource datasource) {
//        DatasetFormatConfig formatConfig = DatasetFormatConfig.builder().build();
//
//        // 数据集位置信息映射
//        DatasetLocationConfig locationConfig = DatasetLocationConfig.builder()
//            .datasourceId(domainData.getDatasourceId())
//            .build();
//        switch (datasource.getConnConfig().getType()) {
//            case LOCAL_HOST:
//            case OSS:
//            case MINIO:
//            case OBS:
//                locationConfig.setLocationConfig(FileSystemLocationConfig.builder()
//                    .relativePath(domainData.getRelativeUri())
//                    .build());
//
//                if (domainData.getFileFormat() == Common.FileFormat.CSV) {
//                    formatConfig.setType(DatasetFormatTypeEnum.CSV);
//                    formatConfig.setFormatConfig(CSVFormatConfig.builder().build());
//                } else {
//                    formatConfig.setType(DatasetFormatTypeEnum.BINARY_FILE);
//                }
//                break;
//            case MYSQL: {
//                locationConfig.setLocationConfig(MysqlLocationConfig.builder()
//                    .table(domainData.getRelativeUri())
//                    .build());
//                formatConfig.setType(DatasetFormatTypeEnum.TABLE);
//                break;
//            }
//            case ODPS:
//                locationConfig.setLocationConfig(OdpsTableInfo.fromKusciaData(domainData));
//                if (domainData.getFileFormat() == Common.FileFormat.CSV ) {
//                    formatConfig.setType(DatasetFormatTypeEnum.TABLE);
//                } else {
//                    formatConfig.setType(DatasetFormatTypeEnum.BINARY_FILE);
//                }
//                break;
//            default:
//                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "不支持的数据源类型 " + datasource.getConnConfig().getType());
//        }
//
//        DatasetSchema datasetSchema = DatasetSchema.builder().build();
//        switch (domainData.getType()) {
//            case "table": {
//                datasetSchema.setType(DatasetSchemaTypeEnum.STRUCTURED_DATA);
//                if (CollectionUtils.isNotEmpty(domainData.getColumnsList())) {
//
//                    Schema schema = new Schema(domainData.getColumnsList().stream()
//                        .map(column ->
//                            Field.nullable(column.getName(), parseArrowTypeFrom(column.getType())))
//                        .collect(Collectors.toList()));
//                    datasetSchema.setArrowSchema(schema);
//                }
//                break;
//            }
//            case "model", "report": {
//                datasetSchema.setType(DatasetSchemaTypeEnum.BINARY);
//                break;
//            }
//            default:
//                datasetSchema.setType(DatasetSchemaTypeEnum.BINARY);
//                break;
//        }
//
//        return Dataset.builder()
//            .datasetId(domainData.getDomaindataId())
//            .name(domainData.getName())
//            .locationConfig(locationConfig)
//            .schema(datasetSchema)
//            .formatConfig(formatConfig)
//            .ownerId(domainData.getVendor())
//            .build();
//    }
//
//
//
//    public static FlightContentFormatConfig fromProto(Flightdm.ContentType contentType) {
//        FlightContentFormatConfig formatConfig = FlightContentFormatConfig.builder().build();
//
//        switch (contentType) {
//            case CSV:
//                formatConfig.setFormatType(FlightContentFormatTypeEnum.CSV);
//                formatConfig.setFormatConfig(CSVFormatConfig.builder().build());
//                break;
//            case RAW:
//                formatConfig.setFormatType(FlightContentFormatTypeEnum.BINARY_FILE);
//                break;
//            case Table:
//            default:
//                formatConfig.setFormatType(FlightContentFormatTypeEnum.STRUCTURED_DATA);
//                break;
//        }
//
//        return formatConfig;
//    }
}
