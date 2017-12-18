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
public class Workflow {

	public static void run(
		SparkSession spark, Dataset<Row> analyticsEventDataSet) {

		Dataset<Row> workflowExistDataSet = doLoad(spark);

		Dataset<Row> workflowNewDataSet =
			doRun(analyticsEventDataSet, workflowExistDataSet);

		doSave(workflowNewDataSet);
	}

	protected static Dataset<Row> doLoad(SparkSession spark) {

		return spark.read().format("org.apache.spark.sql.cassandra").options(
			_workflowOptions).load();
	}

	protected static Dataset<Row> doRun(
		Dataset<Row> analyticsEventDataSet, Dataset<Row> workflowExistDataSet) {

		Dataset<Row> workflowNewDataSet =
			kaleoDefinitionCreate(analyticsEventDataSet);

		workflowNewDataSet = kaleoDefinitionUpdate(analyticsEventDataSet).union(
			workflowNewDataSet);

		workflowNewDataSet = kaleoDefinitionRemove(analyticsEventDataSet).union(
			workflowNewDataSet);

		return filterAndRemoveDuplicates(
			workflowExistDataSet, workflowNewDataSet);
	}

	protected static void doSave(Dataset<Row> workflowNewDataSet) {

		workflowNewDataSet.write().format(
			"org.apache.spark.sql.cassandra").options(_workflowOptions).mode(
				SaveMode.Append).save();
	}

	protected static Dataset<Row> filterAndRemoveDuplicates(
		Dataset<Row> workflowExistDataSet, Dataset<Row> workflowNewDataSet) {

		workflowExistDataSet = workflowExistDataSet.select(
			"processid", "analyticskey", "deleted", "active", "title");

		workflowExistDataSet =
			workflowExistDataSet.withColumn("date", lit(null));

		workflowNewDataSet = workflowNewDataSet.select(
			"processid", "analyticskey", "deleted", "active", "title", "date");

		workflowNewDataSet = workflowNewDataSet.union(workflowExistDataSet);

		Column maxDate = max(col("date")).over(
			Window.partitionBy("analyticskey", "processid"));

		return workflowNewDataSet.withColumn("max", maxDate).where(
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

		return analyticsEventDataSet.withColumn("deleted", lit(false));
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

		return analyticsEventDataSet.withColumn("deleted", lit(false));
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

		return analyticsEventDataSet.withColumn("deleted", lit(true));
	}

	private static final Map<String, String> _workflowOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "workflow");
			}
		};
}
