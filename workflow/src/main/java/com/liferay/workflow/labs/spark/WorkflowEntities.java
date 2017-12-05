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

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * @author Inácio Nery
 */
public class WorkflowEntities {

	public static void doRun(Dataset<Row> analyticsEventDataSet) {

		Dataset<Row> workflowEntitiesNewDataSet =
			kaleoDefinitionVersion(analyticsEventDataSet);

		workflowEntitiesNewDataSet =
			kaleoTask(analyticsEventDataSet).union(workflowEntitiesNewDataSet);

		workflowEntitiesNewDataSet.write().format(
			"org.apache.spark.sql.cassandra").options(
				_workflowEntitiesOptions).mode(SaveMode.Append).save();
	}

	protected static Dataset<Row> kaleoDefinitionVersion(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet = analyticsEventDataSet.filter(
			"eventid = 'KALEO_DEFINITION_VERSION_CREATE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("eventproperties").getField("kaleoDefinitionVersionId").as(
				"id"),
			col("eventproperties").getField("name").as("name"));

		analyticsEventDataSet = analyticsEventDataSet.withColumn(
			"entity", lit("KALEO_DEFINITION_VERSION"));

		return analyticsEventDataSet.select("entity", "id", "name");
	}

	protected static Dataset<Row> kaleoTask(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet =
			analyticsEventDataSet.filter("eventid = 'KALEO_TASK_CREATE'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("eventproperties").getField("kaleoTaskId").as("id"),
			col("eventproperties").getField("name").as("name"));

		analyticsEventDataSet =
			analyticsEventDataSet.withColumn("entity", lit("KALEO_TASK"));

		return analyticsEventDataSet.select("entity", "id", "name");
	}

	private static final Map<String, String> _workflowEntitiesOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "workflowentities");
			}
		};

}
