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
public class WorkflowProcessAvg {

	public static void run(
		SparkSession spark, Dataset<Row> analyticsEventDataSet) {

		Dataset<Row> workflowProcessAvgExistDataSet = doLoad(spark);

		Dataset<Row> workflowProcessAvgNewDataSet =
			doRun(analyticsEventDataSet, workflowProcessAvgExistDataSet);

		doSave(workflowProcessAvgNewDataSet);
	}

	protected static Dataset<Row> doLoad(SparkSession spark) {

		return spark.read().format("org.apache.spark.sql.cassandra").options(
			_workflowProcessAvgOptions).load();
	}

	protected static Dataset<Row> doRun(
		Dataset<Row> analyticsEventDataSet,
		Dataset<Row> workflowProcessAvgExistDataSet) {

		Dataset<Row> workflowProcessAvgNewDataSet =
			kaleoInstanceCreate(analyticsEventDataSet);

		workflowProcessAvgNewDataSet =
			kaleoInstanceComplete(analyticsEventDataSet).union(
				workflowProcessAvgNewDataSet);

		workflowProcessAvgNewDataSet =
			kaleoInstanceRemove(analyticsEventDataSet).union(
				workflowProcessAvgNewDataSet);

		workflowProcessAvgNewDataSet = filterAndGroupBy(
			workflowProcessAvgExistDataSet, workflowProcessAvgNewDataSet);

		return workflowProcessAvgNewDataSet;
	}

	protected static void doSave(Dataset<Row> workflowProcessAvgNewDataSet) {

		workflowProcessAvgNewDataSet.write().format(
			"org.apache.spark.sql.cassandra").options(
				_workflowProcessAvgOptions).mode(SaveMode.Append).save();
	}

	protected static Dataset<Row> filterAndGroupBy(
		Dataset<Row> workflowProcessAvgExistDataSet,
		Dataset<Row> workflowProcessAvgNewDataSet) {

		workflowProcessAvgExistDataSet = workflowProcessAvgExistDataSet.select(
			"date", "analyticskey", "processid", "processversionid",
			"totalcompleted", "totalduration", "totalremoved", "totalstarted");

		workflowProcessAvgNewDataSet = workflowProcessAvgNewDataSet.select(
			"date", "analyticskey", "processid", "processversionid",
			"totalcompleted", "totalduration", "totalremoved", "totalstarted");

		workflowProcessAvgNewDataSet =
			workflowProcessAvgNewDataSet.union(workflowProcessAvgExistDataSet);

		return workflowProcessAvgNewDataSet.groupBy(
			"date", "analyticskey", "processid", "processversionid").agg(
				sum("totalcompleted").as("totalcompleted"),
				sum("totalduration").as("totalduration"),
				sum("totalremoved").as("totalremoved"),
				sum("totalstarted").as("totalstarted"));
	}

	protected static Dataset<Row> kaleoInstanceComplete(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_INSTANCE_COMPLETE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("eventproperties").getField("date").cast("date").as("date"),
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionId").as(
				"processid"),
			col("eventproperties").getField("kaleoDefinitionVersionId").as(
				"processversionid"),
			col("eventproperties").getField("duration").as("totalduration"));

		return analyticsEventDataSet.withColumn(
			"totalcompleted", lit(1)).withColumn(
				"totalremoved", lit(0)).withColumn("totalstarted", lit(0));
	}

	protected static Dataset<Row> kaleoInstanceCreate(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_INSTANCE_CREATE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("eventproperties").getField("date").cast("date").as("date"),
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionId").as(
				"processid"),
			col("eventproperties").getField("kaleoDefinitionVersionId").as(
				"processversionid"));

		return analyticsEventDataSet.withColumn(
			"totalduration", lit(0)).withColumn(
				"totalcompleted", lit(0)).withColumn(
					"totalremoved", lit(0)).withColumn("totalstarted", lit(1));
	}

	protected static Dataset<Row> kaleoInstanceRemove(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_INSTANCE_REMOVE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("eventproperties").getField("date").cast("date").as("date"),
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionId").as(
				"processid"),
			col("eventproperties").getField("kaleoDefinitionVersionId").as(
				"processversionid"));

		return analyticsEventDataSet.withColumn(
			"totalduration", lit(0)).withColumn(
				"totalcompleted", lit(0)).withColumn(
					"totalremoved", lit(1)).withColumn("totalstarted", lit(0));
	}

	private final static Map<String, String> _workflowProcessAvgOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "workflowprocessavg");
			}
		};
}
