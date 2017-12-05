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
public class WorkflowProcessTroughput {

	public static void doRun(
		Dataset<Row> analyticsEventDataSet,
		Dataset<Row> workflowProcessAvgExistDataSet) {

		Dataset<Row> workflowProcessAvgNewDataSet =
			kaleoInstanceComplete(analyticsEventDataSet);

		workflowProcessAvgNewDataSet =
			kaleoInstanceCreate(analyticsEventDataSet).union(
				workflowProcessAvgNewDataSet);

		workflowProcessAvgNewDataSet =
			kaleoInstanceRemove(analyticsEventDataSet).union(
				workflowProcessAvgNewDataSet);

		workflowProcessAvgNewDataSet = filterAndGroupBy(
			workflowProcessAvgExistDataSet, workflowProcessAvgNewDataSet);

		workflowProcessAvgNewDataSet.write().format(
			"org.apache.spark.sql.cassandra").options(
				_workflowProcessAvgOptions).mode(SaveMode.Append).save();
	}

	protected static Dataset<Row> filterAndGroupBy(
		Dataset<Row> workflowProcessAvgExistDataSet,
		Dataset<Row> workflowProcessAvgNewDataSet) {

		workflowProcessAvgExistDataSet = workflowProcessAvgExistDataSet.filter(
			col("analyticskey").isin(
				workflowProcessAvgNewDataSet.select("analyticskey").columns()));

		workflowProcessAvgExistDataSet = workflowProcessAvgExistDataSet.filter(
			col("processversionid").isin(
				workflowProcessAvgNewDataSet.select(
					"processversionid").columns()));

		workflowProcessAvgExistDataSet = workflowProcessAvgExistDataSet.filter(
			col("date").isin(
				workflowProcessAvgNewDataSet.select("date").columns()));

		workflowProcessAvgExistDataSet = workflowProcessAvgExistDataSet.select(
			"date", "analyticskey", "processversionid", "total",
			"totalcompleted", "totalduration", "totalremoved");

		workflowProcessAvgNewDataSet =
			workflowProcessAvgNewDataSet.union(workflowProcessAvgExistDataSet);

		return workflowProcessAvgNewDataSet.groupBy(
			"date", "analyticskey", "processversionid").agg(
				sum("total").as("total"),
				sum("totalcompleted").as("totalcompleted"),
				sum("totalremoved").as("totalremoved"),
				sum("totalduration").as("totalduration"));
	}

	protected static Dataset<Row> kaleoInstanceComplete(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_INSTANCE_COMPLETE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("eventproperties").getField("date").cast("date").as("date"),
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionVersionId").as(
				"processversionid"),
			col("eventproperties").getField("duration").as("totalduration"));

		analyticsEventDataSet = analyticsEventDataSet.withColumn(
			"totalcompleted", lit(1)).withColumn(
				"totalremoved", lit(0)).withColumn("total", lit(0));

		return analyticsEventDataSet.select(
			"date", "analyticskey", "processversionid", "total",
			"totalcompleted", "totalduration", "totalremoved");
	}

	protected static Dataset<Row> kaleoInstanceCreate(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_INSTANCE_CREATE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("eventproperties").getField("date").cast("date").as("date"),
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionVersionId").as(
				"processversionid"));

		analyticsEventDataSet = analyticsEventDataSet.withColumn(
			"totalduration", lit(0)).withColumn(
				"totalcompleted", lit(0)).withColumn(
					"totalremoved", lit(0)).withColumn("total", lit(1));

		return analyticsEventDataSet.select(
			"date", "analyticskey", "processversionid", "total",
			"totalcompleted", "totalduration", "totalremoved");
	}

	protected static Dataset<Row> kaleoInstanceRemove(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_INSTANCE_REMOVE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("eventproperties").getField("date").cast("date").as("date"),
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionVersionId").as(
				"processversionid"));

		analyticsEventDataSet = analyticsEventDataSet.withColumn(
			"totalduration", lit(0)).withColumn(
				"totalcompleted", lit(0)).withColumn(
					"totalremoved", lit(1)).withColumn("total", lit(0));

		return analyticsEventDataSet.select(
			"date", "analyticskey", "processversionid", "total",
			"totalcompleted", "totalduration", "totalremoved");
	}

	private final static Map<String, String> _workflowProcessAvgOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "workflowprocessavg");
			}
		};
}
