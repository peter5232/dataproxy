package org.secretflow.dataproxy.plugin.database.config;


import com.fasterxml.jackson.annotation.JsonIgnore;
import org.secretflow.v1alpha1.common.Common;

import java.util.List;
/*
 * @param tablename：表名
 * @param partition：所选的列（domaindata 并不一定需要表内所有的列）
 */
public record DatabaseTableConfig(String tableName, String partition, @JsonIgnore List<Common.DataColumn> columns) {
}
