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
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.max;
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
 * @author Rafael Praxedes
 * @author Leonardo Barros
 */
/*
 * FIELD_BLUR, FIELD_EMPTY, FIELD_FOCUS, FIELD_LOADED,
 * FIELD_STARTED_FILLING, FIELD_VALIDATION_ERROR, FORM_PAGE_SHOW,
 * FORM_PAGE_HIDE, FORM_VALIDATION_ERROR, FORM_VIEW, FORM_SUBMIT
 */
public class FormsAnalyticsJob {

	private static SparkSession createSparkSession() {
		SparkSession sparkSession = SparkSession.builder()
				.appName("Forms View Started")
				.config("spark.cassandra.connection.host", "192.168.108.90")
				.config("spark.cassandra.auth.username", "cassandra")
				.config("spark.cassandra.auth.password", "cassandra")
					.master("local[*]").getOrCreate();

		sparkSession.sparkContext().setLogLevel("ERROR");

		return sparkSession;
	}

	protected static void doRun() {
		SparkSession sparkSession = createSparkSession();

		OffsetDateTime referenceDate = 
			OffsetDateTime.now().minusMinutes(240);

		Dataset<Row> analyticsEventOld = getDataset(sparkSession, referenceDate, true);

		unionAndSaveAggregatedDataset(
			sparkSession, 
			runConverted(sparkSession, referenceDate),
			runConvertedTime(sparkSession, referenceDate),
			runSessions(sparkSession, referenceDate),
			runViewsStarted(sparkSession, analyticsEventOld, referenceDate)
		);

		sparkSession.stop();
	}

	protected static Dataset<Row> runConvertedTime(
		SparkSession sparkSession, OffsetDateTime referenceDate) {

		Dataset<Row> analyticsEvents = loadDataset(sparkSession);
		
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
		
		Dataset<Row> analyticsEventNew = getDataset(sparkSession, referenceDate, false);

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

	protected static Dataset<Row> runSessions(
		SparkSession sparkSession, OffsetDateTime referenceDate) {
		
		Dataset<Row> analyticsEventNew = getDataset(sparkSession, referenceDate, false);
		
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

	protected static Dataset<Row> runConverted(
		SparkSession sparkSession, OffsetDateTime referenceDate) {

		Dataset<Row> analyticsEventNew =
			getDataset(sparkSession, referenceDate, false);

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

	protected static Dataset<Row> runViewsStarted(
		SparkSession sparkSession, Dataset<Row> analyticsEventOld, OffsetDateTime referenceDate) {

		Dataset<Row> analyticsEventNew = getDataset(sparkSession, referenceDate, false);

		Column formIdColumn = 
			analyticsEventNew.col("eventproperties").getField("formId").equalTo(
				analyticsEventOld.col("eventproperties").getField("formId"));

		Column userIdColumn = 
			analyticsEventNew.col("eventproperties").getField("userId").equalTo(
				analyticsEventOld.col("eventproperties").getField("userId"));

		Column eventColumn = 
			analyticsEventNew.col("eventid").equalTo(
				analyticsEventOld.col("eventid"));

		Dataset<FormEvent> viewDataset = analyticsEventNew.joinWith(
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

				return new FormEvent(
					Long.parseLong(properties.get("userId").toString()),
					Long.parseLong(properties.get("formId").toString()),
					tuple._1.getString(4), tuple._1.getTimestamp(1), 
					tuple._1.getString(3));
			}, 
			Encoders.bean(FormEvent.class)
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

	@SafeVarargs
	protected static void unionAndSaveAggregatedDataset(
		SparkSession sparkSession, Dataset<Row>...datasets) {

		Dataset<Row> loadedDataset = loadAggregatedDataset(sparkSession);

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
			sum("dropoffs").as("dropoffs")
		);

		saveFormsAggregatedData(datasetToSave);
	}
	
	protected static void saveFormsAggregatedData(Dataset<Row> dataset) {
		dataset.write()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "formsaggregateddata")
			.mode(SaveMode.Append)
			.save();
	}
	
	protected static Column[] getFormsAggregatedDataColumns() {
		return new Column[] {
			col("analyticskey"), col("formid"), col("date"),
			col("views"), col("sessions"), col("started"),
			col("converted"), col("convertedtotaltime"),
			col("dropoffs")
		};
	}
	
	protected static Dataset<Row> loadAggregatedDataset(SparkSession sparkSession) {
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

	protected static Dataset<Row> loadDataset(SparkSession sparkSession) {
		return sparkSession.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "analyticsevent")
			.load();
	}

	protected static Dataset<Row> getDataset(
		SparkSession sparkSession, OffsetDateTime referenceDate, boolean beforeReferenceDate) {

		Dataset<Row> analyticsEventOld = loadDataset(sparkSession);

		if(beforeReferenceDate) {
			analyticsEventOld = analyticsEventOld.filter(
				date_format(col("createdate"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").leq(referenceDate.toString()).
				and(col("applicationid").equalTo("com.liferay.dynamic.data.mapping.forms.analytics:1.0.0"))
			);
		}
		else {
			analyticsEventOld = analyticsEventOld.filter(
				date_format(col("createdate"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").gt(referenceDate.toString()).
				and(col("applicationid").equalTo("com.liferay.dynamic.data.mapping.forms.analytics:1.0.0"))
			);
		}
		
		return analyticsEventOld;
	}
}
