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

package org.secretflow.dataproxy.plugin.odps.reader;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.InstanceTunnel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.dataproxy.core.param.ParamWrapper;
import org.secretflow.dataproxy.core.service.TicketService;
import org.secretflow.dataproxy.core.service.impl.CacheTicketService;
import org.secretflow.dataproxy.plugin.odps.config.OdpsCommandConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsConfigConstant;
import org.secretflow.dataproxy.plugin.odps.config.OdpsConnectConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsTableConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsTableQueryConfig;
import org.secretflow.dataproxy.plugin.odps.config.ScqlCommandJobConfig;
import org.secretflow.dataproxy.plugin.odps.config.TaskConfig;
import org.secretflow.dataproxy.plugin.odps.constant.OdpsTypeEnum;
import org.secretflow.dataproxy.plugin.odps.utils.OdpsUtil;
import org.secretflow.v1alpha1.common.Common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author yuexie
 * @date 2024/11/26 13:48
 **/
@Slf4j
public class OdpsDoGetContext {

    private final OdpsCommandConfig<?> odpsCommandConfig;

    @Getter
    private InstanceTunnel.DownloadSession downloadSession;

    @Getter
    private long count;

    @Getter
    private Schema schema;


    public OdpsDoGetContext(OdpsCommandConfig<?> config) {
        this.odpsCommandConfig = config;
        prepare();
    }

    private void prepare() {

        // 2. init download session
        try {
            OdpsConnectConfig odpsConnectConfig = odpsCommandConfig.getOdpsConnectConfig();
            Odps odps = OdpsUtil.initOdps(odpsConnectConfig);
            String querySql;

            if (odpsCommandConfig instanceof ScqlCommandJobConfig scqlReadJobConfig) {
                querySql = scqlReadJobConfig.getCommandConfig();
            } else if (odpsCommandConfig instanceof OdpsTableQueryConfig odpsTableQueryConfig) {
                OdpsTableConfig tableConfig = odpsTableQueryConfig.getCommandConfig();
                // If the DomainData is parsed to SQL,
                // the query is performed by the field of the DomainData and the field value of the column of the DomainData is returned
                querySql = this.buildSql(odps, tableConfig.tableName(), tableConfig.columns().stream().map(Common.DataColumn::getName).toList(), tableConfig.partition());
                this.schema = odpsCommandConfig.getResultSchema();

            } else {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Unsupported read parameter type: " + odpsCommandConfig.getClass());
            }

            Instance runInstance = SQLTask.run(odps, odpsConnectConfig.projectName(), querySql, OdpsUtil.getSqlFlag(), null);

            runInstance.waitForSuccess();
            log.debug("SQL Task run success, sql: {}", querySql);

            if (runInstance.isSuccessful()) {
                downloadSession = new InstanceTunnel(odps).createDownloadSession(odpsConnectConfig.projectName(), runInstance.getId(), false);
                this.count = downloadSession.getRecordCount();
            } else {
                log.error("SQL Task run result is not successful, sql: {}", querySql);
                throw DataproxyException.of(DataproxyErrorCode.ODPS_ERROR, "SQL Task run result is not successful, sql: " + querySql);
            }

            // 3. init schema
            // If the SQL is directly queried, the decision returned by SQL in the returned arrow schema is initialized here
            // If it is determined by table, the field defined by DomainData is returned and initialized in a different way
            if (odpsCommandConfig.getOdpsTypeEnum() == OdpsTypeEnum.SQL) {
                this.initArrowSchemaFromColumns();
            }

        } catch (OdpsException e) {
            log.error("SQL Task run error", e);
            throw DataproxyException.of(DataproxyErrorCode.ODPS_ERROR, "SQL Task run error", e);
        }
    }

    public List<TaskConfig> getTaskConfigs() {
        long upgradeThreshold = FlightServerContext.getOrDefault(
                OdpsConfigConstant.ConfigKey.FLIGHT_ENDPOINT_UPGRADE_TO_MULTI_BATCH_THRESHOLD,
                Long.class,
                1_000_000L);
        int numberOfParts = FlightServerContext.getOrDefault(
                OdpsConfigConstant.ConfigKey.MAX_FLIGHT_ENDPOINT,
                Integer.class,
                3);
        if (this.count > upgradeThreshold) {

            // More than 3 million rows, and the task needs to be split
            log.info("SQL result count is greater than {}, split into {} tasks", upgradeThreshold, numberOfParts);

            long itemsPerBatch = count / numberOfParts;
            long remainder = count % numberOfParts;

            ArrayList<TaskConfig> taskConfigs = new ArrayList<>();
            for (int i = 0; i < numberOfParts; i++) {
                taskConfigs.add(
                        new TaskConfig(this,
                                i * itemsPerBatch + Math.min(i, remainder),
                                itemsPerBatch + (i < remainder ? 1 : 0)));
            }

            return taskConfigs;
        }

        return Collections.singletonList(new TaskConfig(this, 0, count));
    }

    public List<FlightEndpoint> getFlightEndpoints(String type) {
        final TicketService ticketService = CacheTicketService.getInstance();
        byte[] bytes;
        List<TaskConfig> taskConfigs = getTaskConfigs();
        List<FlightEndpoint> endpointList = new ArrayList<>(taskConfigs.size());
        for (TaskConfig taskConfig : taskConfigs) {
            log.info("taskConfig: {}", taskConfig);
            bytes = ticketService.generateTicket(ParamWrapper.of(type, taskConfig));

            endpointList.add(new FlightEndpoint(new Ticket(bytes), FlightServerContext.getInstance().getFlightServerConfig().getLocation()));
        }

        return endpointList;
    }

    private void initArrowSchemaFromColumns() {
        schema = new Schema(downloadSession.getSchema().getAllColumns().stream().map(OdpsUtil::convertOdpsColumnToArrowField).toList());
    }

    private String buildSql(Odps odps, String tableName, List<String> fields, String whereClause) {

        final Pattern columnOrValuePattern = Pattern.compile("^[\\u00b7A-Za-z0-9\\u4e00-\\u9fa5\\-_,.]*$");

        if (!columnOrValuePattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        boolean partitioned = odps.tables().get(tableName).isPartitioned();
        // Common tables no longer concatenate conditional statements
        if (!partitioned) {
            whereClause = "";
        }

        if (!whereClause.isEmpty()) {
            String[] groups = whereClause.split("[,/]");
            if (groups.length > 1) {
                final PartitionSpec partitionSpec = new PartitionSpec(whereClause);

                for (String key : partitionSpec.keys()) {
                    if (!columnOrValuePattern.matcher(key).matches()) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + key);
                    }
                    if (!columnOrValuePattern.matcher(partitionSpec.get(key)).matches()) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition value:" + partitionSpec.get(key));
                    }
                }

                List<String> list = partitionSpec.keys().stream().map(k -> k + "='" + partitionSpec.get(k) + "'").toList();
                whereClause = String.join(" and ", list);
            }
        }

        log.debug("whereClause: {}", whereClause);

        return "select " + String.join(",", fields) + " from " + tableName + (whereClause.isEmpty() ? "" : " where " + whereClause) + ";";
    }

}
