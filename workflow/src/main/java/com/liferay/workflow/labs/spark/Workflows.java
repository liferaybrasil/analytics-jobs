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
import static org.apache.spark.sql.functions.max;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

/**
 * @author Inácio Nery
 */
public class Workflows {

	public static void run(
		SparkSession spark, Dataset<Row> analyticsEventDataSet) {

		Dataset<Row> workflowsExistDataSet = doLoad(spark);

		Dataset<Row> workflowsNewDataSet =
			doRun(analyticsEventDataSet, workflowsExistDataSet);

		doSave(workflowsNewDataSet);
	}

	protected static Dataset<Row> doLoad(SparkSession spark) {

		return spark.read().format("org.apache.spark.sql.cassandra").options(
			_workflowsOptions).load();
	}

	protected static Dataset<Row> doRun(
		Dataset<Row> analyticsEventDataSet,
		Dataset<Row> workflowsExistDataSet) {

		Dataset<Row> workflowsNewDataSet =
			kaleoDefinitionCreate(analyticsEventDataSet);

		workflowsNewDataSet =
			kaleoDefinitionUpdate(analyticsEventDataSet).union(
				workflowsNewDataSet);

		workflowsNewDataSet =
			kaleoDefinitionRemove(analyticsEventDataSet).union(
				workflowsNewDataSet);

		return filterAndRemoveDuplicates(
			workflowsExistDataSet, workflowsNewDataSet);
	}

	protected static void doSave(Dataset<Row> workflowsNewDataSet) {

		workflowsNewDataSet.write().format(
			"org.apache.spark.sql.cassandra").options(_workflowsOptions).mode(
				SaveMode.Append).save();
	}

	protected static Dataset<Row> filterAndRemoveDuplicates(
		Dataset<Row> workflowsExistDataSet, Dataset<Row> workflowsNewDataSet) {

		workflowsExistDataSet = workflowsExistDataSet.filter(
			col("analyticskey").isin(
				workflowsNewDataSet.select("analyticskey").columns()));

		workflowsExistDataSet = workflowsExistDataSet.filter(
			col("processid").isin(
				workflowsNewDataSet.select("processid").columns()));

		workflowsExistDataSet = workflowsExistDataSet.select(
			"processid", "analyticskey", "deleted", "active", "title");

		workflowsExistDataSet =
			workflowsExistDataSet.withColumn("date", lit(null));

		workflowsNewDataSet = workflowsNewDataSet.union(workflowsExistDataSet);

		Column maxDate = max(col("date")).over(
			Window.partitionBy("analyticskey", "processid"));

		return workflowsNewDataSet.withColumn("max", maxDate).where(
			col("date").geq(col("max"))).select(
				"processid", "analyticskey", "deleted", "active", "title");
	}

	protected static Dataset<Row> kaleoDefinitionCreate(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_DEFINITION_CREATE'");

		analyticsEventDataSet =
			analyticsEventDataSet.select(
				col("analyticskey"),
				col("eventproperties").getField("kaleoDefinitionId").as(
					"processid"),
				col("eventproperties").getField("title").as("title"),
				col("eventproperties").getField("active").cast("boolean").as(
					"active"),
				col("eventproperties").getField("date").as("date"));

		analyticsEventDataSet =
			analyticsEventDataSet.withColumn("deleted", lit(false));

		return analyticsEventDataSet.select(
			"processid", "analyticskey", "deleted", "active", "title", "date");
	}

	protected static Dataset<Row> kaleoDefinitionUpdate(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_DEFINITION_UPDATE'");

		analyticsEventDataSet =
			analyticsEventDataSet.select(
				col("analyticskey"),
				col("eventproperties").getField("kaleoDefinitionId").as(
					"processid"),
				col("eventproperties").getField("title").as("title"),
				col("eventproperties").getField("active").cast("boolean").as(
					"active"),
				col("eventproperties").getField("date").as("date"));

		analyticsEventDataSet =
			analyticsEventDataSet.withColumn("deleted", lit(false));

		return analyticsEventDataSet.select(
			"processid", "analyticskey", "deleted", "active", "title", "date");
	}

	protected static Dataset<Row> kaleoDefinitionRemove(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_DEFINITION_REMOVE'");

		analyticsEventDataSet =
			analyticsEventDataSet.select(
				col("analyticskey"),
				col("eventproperties").getField("kaleoDefinitionId").as(
					"processid"),
				col("eventproperties").getField("title").as("title"),
				col("eventproperties").getField("active").cast("boolean").as(
					"active"),
				col("eventproperties").getField("date").as("date"));

		analyticsEventDataSet =
			analyticsEventDataSet.withColumn("deleted", lit(true));

		return analyticsEventDataSet.select(
			"processid", "analyticskey", "deleted", "active", "title", "date");
	}

	private static final Map<String, String> _workflowsOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "workflows");
			}
		};
}
