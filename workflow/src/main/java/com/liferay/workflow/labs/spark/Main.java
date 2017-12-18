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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

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

		SparkSession spark = SparkSession.builder().appName("Workflow").config(
			"spark.cassandra.connection.host", "192.168.108.90").config(
				"spark.cassandra.auth.username", "cassandra").config(
					"spark.cassandra.auth.password", "cassandra").getOrCreate();

		Dataset<Row> analyticsEventDataSet =
			spark.read().format("org.apache.spark.sql.cassandra").options(
				_analyticsEventOptions).load();

		analyticsEventDataSet = analyticsEventDataSet.filter(
			date_format(col("createdate"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").geq(
				_last5Minutes));

		analyticsEventDataSet = analyticsEventDataSet.filter(
			"applicationid = '" + _applicationId + "'");

		WorkflowProcessAvg.run(spark, analyticsEventDataSet);

		WorkflowTaskAvg.run(spark, analyticsEventDataSet);

		WorkflowEntities.run(spark, analyticsEventDataSet);

		Workflows.run(spark, analyticsEventDataSet);

		spark.stop();
	}

	private static final Map<String, String> _analyticsEventOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "analyticsevent");
			}
		};

	private static final String _applicationId =
		"com.liferay.portal.workflow.analytics:1.0.0";

	private final static String _last5Minutes =
		OffsetDateTime.now().minusMinutes(5).toString();

}
