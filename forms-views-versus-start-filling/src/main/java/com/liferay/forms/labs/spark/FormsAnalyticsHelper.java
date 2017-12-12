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
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.unix_timestamp;

import java.time.OffsetDateTime;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author Leonardo Barros
 */
public class FormsAnalyticsHelper {

	public FormsAnalyticsHelper(
		SparkSession sparkSession, AnalyticsDataset analyticsDataset) {

		this.analyticsDataset = analyticsDataset;
		this.sparkSession = sparkSession;
	}
	
	public void run(OffsetDateTime referenceDate) {
		unionAndSaveFormsAggregatedDataset(
			runConverted(referenceDate),
			runConvertedTime(referenceDate),
			runDropoffs(referenceDate),
			runSessions(referenceDate),
			runViewsStarted(referenceDate)
		);
	}

	protected Column[] getFormsAggregatedDataColumns() {
		return new Column[] {
			col("analyticskey"), col("formid"), col("date"),
			col("views"), col("sessions"), col("started"),
			col("converted"), col("convertedtotaltime"),
			col("dropoffs")
		};
	}

	protected Dataset<Row> loadAggregatedDataset() {
		return sparkSession.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "formsaggregateddata")
			.load().as(
				Encoders.bean(FormsAggregatedData.class)
			).select(
				getFormsAggregatedDataColumns()
			);
	}

	protected Dataset<Row> runConverted(
		OffsetDateTime referenceDate) {

		Dataset<Row> analyticsEventNew =
			analyticsDataset.getDataset(sparkSession, referenceDate, false);

		analyticsEventNew = analyticsEventNew.filter("eventid = 'FORM_SUBMIT'");

		Dataset<Row> dataset = analyticsEventNew.select(
			col("analyticsKey").as("analyticskey"), 
			col("eventproperties").getField("formId").as("formid"), 
			col("createdate").cast("date").as("date")
		).withColumn(
			"converted", lit(1)
		).groupBy(
			"analyticskey", "formid", "date"
		).agg(
			sum("converted").as("converted")
		).withColumn(
			"views", lit(0)
		).withColumn(
			"started", lit(0)
		).withColumn(
			"sessions", lit(0)
		).withColumn(
			"convertedtotaltime", lit(0)
		).withColumn(
			"dropoffs", lit(0)
		).select(
			getFormsAggregatedDataColumns()
		);

		return dataset;
	}

	protected Dataset<Row> runConvertedTime(
		OffsetDateTime referenceDate) {

		Dataset<Row> analyticsEvents = 
			analyticsDataset.loadDataset(sparkSession);

		analyticsEvents = analyticsEvents.filter(
			(col("applicationid").equalTo("com.liferay.dynamic.data.mapping.forms.analytics:1.0.0")).
			and(col("eventid").equalTo("FORM_VIEW"))
		).select(
			col("analyticskey").as("analyticskey1"),
			col("userid").as("userid1"),
			col("eventproperties").getField("formId").as("formid1"),
			col("eventproperties").getField("formTransaction").as("formtransaction1"),
			col("context").getField("sessionId").as("sessionid1"),
			col("createdate").as("dateview")
		);

		Dataset<Row> analyticsEventNew = 
			analyticsDataset.getDataset(sparkSession, referenceDate, false);

		analyticsEventNew = 
			analyticsEventNew.filter(
				col("eventid").equalTo("FORM_SUBMIT")
			).select(
				col("analyticskey").as("analyticskey2"),
				col("userid").as("userid2"),
				col("eventproperties").getField("formId").as("formid2"),
				col("eventproperties").getField("formTransaction").as("formtransaction2"),
				col("context").getField("sessionId").as("sessionid2"),
				col("createdate").as("datesubmit")
			);

		Column analyticskeyColumn = 
			analyticsEventNew.col("analyticskey2").equalTo(
				analyticsEvents.col("analyticskey1"));

		Column useridColumn = 
			analyticsEventNew.col("userid2").equalTo(
				analyticsEvents.col("userid1"));

		Column formidColumn = 
			analyticsEventNew.col("formid2").equalTo(
				analyticsEvents.col("formid1"));

		Column formtransactionColumn = 
			analyticsEventNew.col("formtransaction2").equalTo(
				analyticsEvents.col("formtransaction1"));

		Column sessionidColumn = 
			analyticsEventNew.col("sessionid2").equalTo(
				analyticsEvents.col("sessionid1"));

		analyticsEventNew = analyticsEventNew.join(
			analyticsEvents, 
			analyticskeyColumn.and(
				useridColumn
			).and(
				formidColumn
			).and(
				formtransactionColumn
			).and(
				sessionidColumn
			)
		).select(
			col("analyticskey2").as("analyticskey"),
			col("userid2").as("userid"),
			col("formId2").as("formid"),
			col("formTransaction2").as("formtransaction"),
			col("datesubmit"),
			col("dateview")
		).withColumn(
			"datediff", 
			unix_timestamp(col("datesubmit")).minus(unix_timestamp(col("dateview")))
		);

		analyticsEventNew = analyticsEventNew.groupBy(
			col("analyticskey"), col("userid"), col("formId"), col("formTransaction")
		).agg(
			max(col("datediff")).as("convertedtotaltime"),
			max(col("datesubmit")).cast("date").as("date")
		).withColumn(
			"views", lit(0)
		).withColumn(
			"started", lit(0)
		).withColumn(
			"converted", lit(0)
		).withColumn(
			"sessions", lit(0)
		).withColumn(
			"dropoffs", lit(0)
		).select(
			getFormsAggregatedDataColumns()
		);

		return analyticsEventNew;
	}

	protected Dataset<Row> runDropoffs(
		OffsetDateTime referenceDate) {

		Dataset<Row> analyticsEvents = analyticsDataset.loadDataset(sparkSession);

		Dataset<Row> analyticsEvents1 = analyticsEvents.filter(
			date_format(col("createdate"), "yyyy-MM-dd").leq(referenceDate.minusDays(1).toString()).
			and(col("applicationid").equalTo("com.liferay.dynamic.data.mapping.forms.analytics:1.0.0"))
		).select(
			col("analyticskey").as("analyticskey1"),
			col("eventproperties").getField("formId").as("formid1"),
			col("eventproperties").getField("userId").as("userid1"),
			col("createdate").cast("date").as("date1"),
			col("eventid").as("eventid1")
		);

		Dataset<Row> analyticsEvents2 = analyticsEvents.filter(
			date_format(col("createdate"), "yyyy-MM-dd").geq(referenceDate.minusDays(1).toString()).
			and(col("applicationid").equalTo("com.liferay.dynamic.data.mapping.forms.analytics:1.0.0"))
		).select(
			col("analyticskey").as("analyticskey2"),
			col("eventproperties").getField("formId").as("formid2"),
			col("eventproperties").getField("userId").as("userid2"),
			col("createdate").cast("date").as("date2"),
			col("eventid").as("eventid2")
		);

		Column analyticskeyColumn = 
			analyticsEvents1.col("analyticskey1").equalTo(
				analyticsEvents2.col("analyticskey2"));

		Column formIdColumn = 
			analyticsEvents1.col("formid1").equalTo(
				analyticsEvents2.col("formid2"));

		Column userIdColumn = 
			analyticsEvents1.col("userid1").equalTo(
				analyticsEvents2.col("userid2"));

		Dataset<FormsDropoff> dataset = analyticsEvents1.joinWith(
			analyticsEvents2, 
			analyticskeyColumn.and(formIdColumn).and(userIdColumn),
			"left_outer"
		).filter(
			tuple -> {
				return tuple._2 == null;
			}
		).map(
			tuple -> {
				return new FormsDropoff(
					Long.parseLong(tuple._1.get(2).toString()),
					Long.parseLong(tuple._1.get(1).toString()),
					tuple._1.getString(4).equals("FORM_SUBMIT") ? 1 : 0,
					tuple._1.getDate(3), tuple._1.getString(0));
			}, 
			Encoders.bean(FormsDropoff.class)
		);

		Dataset<Row> aggregatedDataset = dataset.groupBy(
			col("analyticskey"), col("date"), col("formid"), col("userid") 
		).agg(
			sum("value").as("hasSubmit")
		).filter(
			col("hasSubmit").equalTo(0)
		).withColumn(
			"dropoffs", lit(1)
		).select(
			col("analyticskey"), 
			col("formid"),
			date_add(col("date"), 1).as("date"),
			col("dropoffs")
		).groupBy(
			"analyticskey", "formid", "date"
		).agg(
			sum("dropoffs").as("dropoffs")
		).withColumn(
			"views", lit(0)
		).withColumn(
			"started", lit(0)
		).withColumn(
			"converted", lit(0)
		).withColumn(
			"convertedtotaltime", lit(0)
		).withColumn(
			"sessions", lit(0)
		).select(
			getFormsAggregatedDataColumns()
		);

		return aggregatedDataset;
	}

	protected Dataset<Row> runSessions(
		OffsetDateTime referenceDate) {

		Dataset<Row> analyticsEventNew = 
			analyticsDataset.getDataset(sparkSession, referenceDate, false);

		analyticsEventNew = analyticsEventNew.filter("eventid = 'FORM_VIEW'");

		Dataset<Row> dataset = analyticsEventNew.select(
			col("analyticsKey").as("analyticskey"), 
			col("eventproperties").getField("formId").as("formid"), 
			col("createdate").cast("date").as("date")
		).withColumn(
			"sessions", lit(1)
		).groupBy(
			"analyticskey", "formid", "date"
		).agg(
			sum("sessions").as("sessions")
		).withColumn(
			"views", lit(0)
		).withColumn(
			"started", lit(0)
		).withColumn(
			"converted", lit(0)
		).withColumn(
			"convertedtotaltime", lit(0)
		).withColumn(
			"dropoffs", lit(0)
		).select(
			getFormsAggregatedDataColumns()
		);

		return dataset;
	}

	protected Dataset<Row> runViewsStarted(
		OffsetDateTime referenceDate) {

		Dataset<Row> analyticsEventOld = 
			analyticsDataset.getDataset(sparkSession, referenceDate, true);

		Dataset<Row> analyticsEventNew = 
			analyticsDataset.getDataset(sparkSession, referenceDate, false);

		Column formIdColumn = 
			analyticsEventNew.col("eventproperties").getField("formId").equalTo(
				analyticsEventOld.col("eventproperties").getField("formId"));

		Column userIdColumn = 
			analyticsEventNew.col("eventproperties").getField("userId").equalTo(
				analyticsEventOld.col("eventproperties").getField("userId"));

		Column eventColumn = 
			analyticsEventNew.col("eventid").equalTo(
				analyticsEventOld.col("eventid"));

		Dataset<FormsViewsStarted> viewDataset = analyticsEventNew.joinWith(
			analyticsEventOld, 
			formIdColumn.and(userIdColumn).and(eventColumn),
			"left_outer"
		).filter(
			tuple -> {
				String eventId = tuple._1.getString(4);

				return (eventId.equals("FORM_VIEW") || 
						eventId.equals("FIELD_STARTED_FILLING"))
							&& tuple._2 == null;
			}
		).map(
			tuple -> {
				Map<Object, Object> properties = tuple._1.getJavaMap(7);

				return new FormsViewsStarted(
					Long.parseLong(properties.get("userId").toString()),
					Long.parseLong(properties.get("formId").toString()),
					tuple._1.getString(4), tuple._1.getTimestamp(1), 
					tuple._1.getString(3));
			}, 
			Encoders.bean(FormsViewsStarted.class)
		);

		Dataset<Row> viewDatasetGrouped = viewDataset.groupBy(
			"analyticsKey", "userId", "formId", "event"
		).agg(
			min("date").as("date")
		);

		viewDatasetGrouped = viewDatasetGrouped.select(
			col("analyticsKey").as("analyticskey"), 
			col("formId").as("formid"), 
			col("event"), 
			col("date").cast("date").as("date")
		).withColumn(
			"total", lit(1)
		).groupBy(
			"analyticskey", "formid", "date", "event"
		).agg(
			sum("total").as("total")
		);

		Dataset<Row> dataset = viewDatasetGrouped.map(
			row -> {
				FormsAggregatedData formsAggregatedData = 
					new FormsAggregatedData(
						row.getString(0), row.getLong(1), row.getDate(2));

				if(row.getString(3).equals("FORM_VIEW")) {
					formsAggregatedData.setViews(row.getLong(4));
				}
				else {
					formsAggregatedData.setStarted(row.getLong(4));
				}

				return formsAggregatedData;
			},
			Encoders.bean(FormsAggregatedData.class)
		).select(
			getFormsAggregatedDataColumns()
		);

		return dataset;
	}

	protected void saveFormsAggregatedData(Dataset<Row> dataset) {
		dataset.write()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "formsaggregateddata")
			.mode(SaveMode.Append)
			.save();
	}

	protected void unionAndSaveFormsAggregatedDataset(
		Dataset<Row>...datasets) {

		Dataset<Row> loadedDataset = loadAggregatedDataset();

		for (Dataset<Row> dataset: datasets) {
			loadedDataset = loadedDataset.union(dataset);
		}

		Dataset<Row> datasetToSave = loadedDataset.groupBy(
			"analyticskey", "formid", "date"
		).agg(
			sum("views").as("views"),
			sum("sessions").as("sessions"),
			sum("started").as("started"),
			sum("converted").as("converted"),
			sum("convertedtotaltime").as("convertedtotaltime"),
			max("dropoffs").as("dropoffs")
		);

		saveFormsAggregatedData(datasetToSave);
	}

	private final AnalyticsDataset analyticsDataset;
	private final SparkSession sparkSession;
}
