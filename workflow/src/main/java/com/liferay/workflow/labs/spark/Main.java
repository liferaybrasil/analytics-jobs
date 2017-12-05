/**
 * Copyright (c) 2000-present Liferay, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 */

package com.liferay.workflow.labs.spark;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author Inácio Nery
 */
public class Main {

	public static void main(String[] args) {

		SparkSession spark =
			SparkSession.builder().appName("Workflow").config(
				"spark.cassandra.connection.host", "192.168.108.90").config(
					"spark.cassandra.auth.username", "cassandra").config(
						"spark.cassandra.auth.password",
						"cassandra").getOrCreate();

		Dataset<Row> analyticsEventDataSet =
			spark.read().format("org.apache.spark.sql.cassandra").options(
				_analyticsEventOptions).load();

		analyticsEventDataSet = analyticsEventDataSet.filter(
			"createdate > '" + _last5Minutes + "'");

		Dataset<Row> workflowProcessAvgExistDataSet =
			spark.read().format("org.apache.spark.sql.cassandra").options(
				_workflowProcessAvgOptions).load();

		WorkflowProcessTroughput.doRun(
			analyticsEventDataSet, workflowProcessAvgExistDataSet);

		Dataset<Row> workflowTaskAvgExistDataSet =
			spark.read().format("org.apache.spark.sql.cassandra").options(
				_workflowTaskAvgOptions).load();

		WorkflowTaskTroughput.doRun(
			analyticsEventDataSet, workflowTaskAvgExistDataSet);

		WorkflowEntities.doRun(analyticsEventDataSet);

		spark.stop();
	}
	private final static String _last5Minutes =
		OffsetDateTime.now().minusMinutes(5).toString();

	private static final Map<String, String> _analyticsEventOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "analyticsevent");
			}
		};

	private final static Map<String, String> _workflowProcessAvgOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "workflowprocessavg");
			}
		};

	private static final Map<String, String> _workflowTaskAvgOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "workflowtaskavg");
			}
		};
}
