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

import static org.apache.spark.sql.functions.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * @author Inácio Nery
 */
public class Workflows {

	public static void doRun(
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

		workflowsNewDataSet =
			filterAndGroupBy(workflowsExistDataSet, workflowsNewDataSet);

		workflowsNewDataSet.write().format(
			"org.apache.spark.sql.cassandra").options(_workflowsOptions).mode(
				SaveMode.Append).save();
	}

	protected static Dataset<Row> filterAndGroupBy(
		Dataset<Row> workflowsExistDataSet, Dataset<Row> workflowsNewDataSet) {

		workflowsExistDataSet = workflowsExistDataSet.filter(
			col("analyticskey").isin(
				workflowsNewDataSet.select("analyticskey").columns()));

		workflowsExistDataSet = workflowsExistDataSet.filter(
			col("processid").isin(
				workflowsNewDataSet.select("processid").columns()));

		workflowsExistDataSet = workflowsExistDataSet.select(
			"processid", "analyticskey", "deleted", "active", "title");

		workflowsNewDataSet = workflowsNewDataSet.union(workflowsExistDataSet);

		return workflowsNewDataSet.groupBy("analyticskey", "processid").agg(
			max("active").as("active"), min("deleted").as("deleted"));
	}

	protected static Dataset<Row> kaleoDefinitionCreate(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_DEFINITION_CREATE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionId").as(
				"processid"),
			col("eventproperties").getField("title").as("title"),
			col("eventproperties").getField("active").cast("boolean").as(
				"active"));

		analyticsEventDataSet =
			analyticsEventDataSet.withColumn("deleted", lit(false));

		return analyticsEventDataSet.select(
			"processid", "analyticskey", "deleted", "active", "title");
	}

	protected static Dataset<Row> kaleoDefinitionUpdate(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_DEFINITION_UPDATE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionId").as(
				"processid"),
			col("eventproperties").getField("title").as("title"),
			col("eventproperties").getField("active").cast("boolean").as(
				"active"));

		analyticsEventDataSet =
			analyticsEventDataSet.withColumn("deleted", lit(false));

		return analyticsEventDataSet.select(
			"processid", "analyticskey", "deleted", "active", "title");
	}

	protected static Dataset<Row> kaleoDefinitionRemove(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_DEFINITION_REMOVE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionId").as(
				"processid"),
			col("eventproperties").getField("title").as("title"),
			col("eventproperties").getField("active").cast("boolean").as(
				"active"));

		analyticsEventDataSet =
			analyticsEventDataSet.withColumn("deleted", lit(true));

		return analyticsEventDataSet.select(
			"processid", "analyticskey", "deleted", "active", "title");
	}

	private static final Map<String, String> _workflowsOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "workflows");
			}
		};

}
