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
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * @author Inácio Nery
 */
public class WorkflowTaskAvg {

	public static void doRun(
		Dataset<Row> analyticsEventDataSet,
		Dataset<Row> workflowTaskAvgExistDataSet) {

		Dataset<Row> workflowTaskAvgNewDataSet =
			kaleoTaskInstanceComplete(analyticsEventDataSet);

		workflowTaskAvgNewDataSet = filterAndGroupBy(
			workflowTaskAvgExistDataSet, workflowTaskAvgNewDataSet);

		workflowTaskAvgNewDataSet.write().format(
			"org.apache.spark.sql.cassandra").options(
				_workflowTaskAvgOptions).mode(SaveMode.Append).save();

	}

	protected static Dataset<Row> filterAndGroupBy(
		Dataset<Row> workflowTaskAvgExistDataSet,
		Dataset<Row> workflowTaskAvgNewDataSet) {

		workflowTaskAvgExistDataSet = workflowTaskAvgExistDataSet.filter(
			col("analyticskey").isin(
				workflowTaskAvgNewDataSet.select("analyticskey").columns()));

		workflowTaskAvgExistDataSet = workflowTaskAvgExistDataSet.filter(
			col("taskid").isin(
				workflowTaskAvgNewDataSet.select("taskid").columns()));

		workflowTaskAvgExistDataSet = workflowTaskAvgExistDataSet.filter(
			col("processversionid").isin(
				workflowTaskAvgNewDataSet.select(
					"processversionid").columns()));

		workflowTaskAvgExistDataSet = workflowTaskAvgExistDataSet.filter(
			col("date").isin(
				workflowTaskAvgNewDataSet.select("date").columns()));

		workflowTaskAvgExistDataSet = workflowTaskAvgExistDataSet.select(
			"date", "analyticskey", "taskid", "processversionid", "total",
			"totalduration");

		workflowTaskAvgNewDataSet =
			workflowTaskAvgNewDataSet.union(workflowTaskAvgExistDataSet);

		return workflowTaskAvgNewDataSet.groupBy(
			"date", "analyticskey", "taskid", "processversionid").agg(
				sum("totalduration").as("totalduration"),
				sum("total").as("total"));
	}

	protected static Dataset<Row> kaleoTaskInstanceComplete(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet = analyticsEventDataSet.filter(
			"eventid = 'KALEO_TASK_INSTANCE_TOKEN_COMPLETE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("eventproperties").getField("date").cast("date").as("date"),
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionVersionId").as(
				"processversionid"),
			col("eventproperties").getField("kaleoTaskId").as("taskid"),
			col("eventproperties").getField("duration").as("totalduration"));

		analyticsEventDataSet =
			analyticsEventDataSet.withColumn("total", lit(1));

		return analyticsEventDataSet.select(
			"date", "analyticskey", "taskid", "processversionid", "total",
			"totalduration");
	}

	private static final Map<String, String> _workflowTaskAvgOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "workflowtaskavg");
			}
		};

}
