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

package com.liferay.forms.labs.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

import java.time.OffsetDateTime;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author Leonardo Barros
 */
public class FormFieldsAnalyticsHelper {

	public FormFieldsAnalyticsHelper(
		AnalyticsDataset analyticsDataset, SparkSession sparkSession) {

		this.analyticsDataset = analyticsDataset;
		this.sparkSession = sparkSession;
	}

	public void run(OffsetDateTime referenceDate) {

		unionAndSaveFormFieldsAggregatedDataset(
			runInteractions(referenceDate), runRefilled(referenceDate));
	}

	protected Column[] getFormFieldsAggregatedDataColumns() {
		return new Column[] {
			col("analyticskey"), col("formid"), col("field"),
			col("date"), col("interactions"), col("totaltime"),
			col("empty"), col("refilled"), col("dropoffs")
		};
	}

	protected Dataset<Row> loadAggregatedDataset() {
		return sparkSession.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "formfieldsaggregateddata")
			.load().as(
				Encoders.bean(FormFieldsAggregatedData.class)
			).select(
				getFormFieldsAggregatedDataColumns()
			);
	}

	protected Dataset<Row> runInteractions(
		OffsetDateTime referenceDate) {

		Dataset<Row> dataset =
			analyticsDataset.getDataset(sparkSession, referenceDate, false);

		dataset = dataset.filter(
			col("eventid").equalTo("FIELD_BLUR")
		).select(
			col("analyticsKey").as("analyticskey"), 
			col("eventproperties").getField("formId").as("formid"),
			col("eventproperties").getField("fieldName").as("field"),
			col("createdate").cast("date").as("date"),
			col("eventproperties").getField("interactionTime").as("time")
		).withColumn(
			"interactions", lit(1)
		).groupBy(
			col("analyticskey"), col("formid"), col("date"),
			col("field")
		).agg(
			sum("time").as("totaltime"),
			sum("interactions").as("interactions")
		).withColumn(
			"empty", lit(0)
		).withColumn(
			"refilled", lit(0)
		).withColumn(
			"dropoffs", lit(0)
		).select(
			getFormFieldsAggregatedDataColumns()
		);

		return dataset;
	}

	protected Dataset<Row> runRefilled(
		OffsetDateTime referenceDate) {

		Dataset<Row> dataset =
			analyticsDataset.getDataset(sparkSession, referenceDate, false);

		Dataset<Row> fieldBlurDataset = dataset.filter(
			col("eventid").equalTo("FIELD_BLUR")
		).select(
			col("analyticsKey").as("analyticskey1"), 
			col("eventproperties").getField("formId").as("formid1"),
			col("eventproperties").getField("fieldName").as("field1"),
			col("createdate").cast("date").as("date1")
		).withColumn(
			"totalinteractions", lit(1)
		).groupBy(
			col("analyticskey1"), col("formid1"), col("date1"),
			col("field1")
		).agg(
			sum(col("totalinteractions")).as("totalinteractions")
		).select(
			col("analyticskey1"), col("formid1"), col("field1"),
			col("date1"), col("totalinteractions")
		);

		Dataset<Row> formSubmitDataset = dataset.filter(
			col("eventid").equalTo("FORM_SUBMIT")
		).select(
			col("analyticsKey").as("analyticskey2"), 
			col("eventproperties").getField("formId").as("formid2"),
			col("createdate").cast("date").as("date2")
		).withColumn(
			"totalsubmits", lit(1)
		).groupBy(
			col("analyticskey2"), col("formid2"), col("date2")
		).agg(
			sum(col("totalsubmits")).as("totalsubmits")
		).select(
			col("analyticskey2"), col("formid2"), 
			col("date2"), col("totalsubmits")
		);

		Column analyticskeyColumn =
			fieldBlurDataset.col("analyticskey1").equalTo(
				formSubmitDataset.col("analyticskey2"));

		Column formColumn =
			fieldBlurDataset.col("formid1").equalTo(
				formSubmitDataset.col("formid2"));

		Column dateColumn = 
			fieldBlurDataset.col("date1").equalTo(
				formSubmitDataset.col("date2"));

		Dataset<Row> aggregatedDataset = fieldBlurDataset.join(
			formSubmitDataset, 
			analyticskeyColumn.and(formColumn).and(dateColumn)
		).select(
			col("analyticskey1").as("analyticskey"),
			col("formid1").as("formid"),
			col("date1").as("date"),
			col("field1").as("field"),
			col("totalinteractions").minus(col("totalsubmits")).as("refilled")
		).withColumn(
			"empty", lit(0)
		).withColumn(
			"interactions", lit(0)
		).withColumn(
			"totaltime", lit(0)
		).withColumn(
			"dropoffs", lit(0)
		).select(
			getFormFieldsAggregatedDataColumns()
		);

		return aggregatedDataset;
	}

	protected void saveFormFieldsAggregatedData(Dataset<Row> dataset) {
		dataset.write()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "formfieldsaggregateddata")
			.mode(SaveMode.Append)
			.save();
	}

	protected void unionAndSaveFormFieldsAggregatedDataset(
		Dataset<Row>...datasets) {

		Dataset<Row> loadedDataset = loadAggregatedDataset();

		for (Dataset<Row> dataset: datasets) {
			loadedDataset = loadedDataset.union(dataset);
		}

		Dataset<Row> datasetToSave = loadedDataset.groupBy(
			"analyticskey", "formid", "field", "date"
		).agg(
			sum("interactions").as("interactions"),
			sum("totaltime").as("totaltime"),
			sum("empty").as("empty"),
			sum("refilled").as("refilled"),
			sum("dropoffs").as("dropoffs")
		);

		saveFormFieldsAggregatedData(datasetToSave);
	}

	private final AnalyticsDataset analyticsDataset;
	private final SparkSession sparkSession;
}
