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
import org.apache.spark.sql.SparkSession;

/**
 * @author Inácio Nery
 */
public class WorkflowTask {

	public static void run(
		SparkSession spark, Dataset<Row> analyticsEventDataSet) {

		Dataset<Row> workflowTaskExistDataSet = doLoad(spark);

		Dataset<Row> workflowTaskNewDataSet =
			doRun(analyticsEventDataSet, workflowTaskExistDataSet);

		doSave(workflowTaskNewDataSet);
	}

	protected static Dataset<Row> doLoad(SparkSession spark) {

		return spark.read().format("org.apache.spark.sql.cassandra").options(
			_workflowTaskOptions).load();
	}

	public static Dataset<Row> doRun(
		Dataset<Row> analyticsEventDataSet,
		Dataset<Row> workflowTaskExistDataSet) {

		Dataset<Row> workflowTaskNewDataSet =
			kaleoTaskInstanceComplete(analyticsEventDataSet);

		return filterAndGroupBy(
			workflowTaskExistDataSet, workflowTaskNewDataSet);
	}

	protected static void doSave(Dataset<Row> workflowTaskNewDataSet) {

		workflowTaskNewDataSet.write().format(
			"org.apache.spark.sql.cassandra").options(
				_workflowTaskOptions).mode(SaveMode.Append).save();
	}

	protected static Dataset<Row> filterAndGroupBy(
		Dataset<Row> workflowTaskExistDataSet,
		Dataset<Row> workflowTaskNewDataSet) {

		workflowTaskExistDataSet = workflowTaskExistDataSet.select(
			"analyticskey", "date", "processversionid", "taskid", "name",
			"total", "totalduration");

		workflowTaskNewDataSet = workflowTaskNewDataSet.select(
			"analyticskey", "date", "processversionid", "taskid", "name",
			"total", "totalduration");
		workflowTaskNewDataSet =
			workflowTaskNewDataSet.union(workflowTaskExistDataSet);

		return workflowTaskNewDataSet.groupBy(
			"date", "analyticskey", "taskid", "processversionid", "name").agg(
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
			col("eventproperties").getField("name"),
			col("eventproperties").getField("duration").as("totalduration"));

		return analyticsEventDataSet.withColumn("total", lit(1));
	}

	private static final Map<String, String> _workflowTaskOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "workflowtask");
			}
		};
}
