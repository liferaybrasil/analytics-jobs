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
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author Rafael Praxedes
 */
/*
 * FIELD_BLUR, FIELD_EMPTY, FIELD_FOCUS, FIELD_LOADED,
 * FIELD_STARTED_FILLING, FIELD_VALIDATION_ERROR, FORM_PAGE_SHOW,
 * FORM_PAGE_HIDE, FORM_VALIDATION_ERROR, FORM_VIEW, FORM_SUBMIT
 */
public class FormsVisitedStartFillingTransformation {
	
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

		Date last5Minutes =
			Date.from(
				Instant.now()
					.plus(3, ChronoUnit.HOURS)
					.minus(5, ChronoUnit.MINUTES));

		runIntermediateTransformation(
			sparkSession, "FORM_VIEW", "formuserview", last5Minutes);

		runIntermediateTransformation(
			sparkSession, "FIELD_STARTED_FILLING", "formuserstartedfilling",
			last5Minutes);

		runIntermediateTransformation(
			sparkSession, "FORM_SUBMIT", "formusersubmitted",
			last5Minutes);

		runMainTransformation(sparkSession, last5Minutes);

		sparkSession.stop();
	}

	protected static void runMainTransformation(
		SparkSession sparkSession, Date last5Minutes) {

		Dataset<Row> userViewEvents = sparkSession.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "formuserview")
			.load();

		userViewEvents = userViewEvents.filter(
			"createdate > '" + last5Minutes + "'");

		userViewEvents =
			userViewEvents.select("formid").withColumn("views", lit(1));

		userViewEvents = userViewEvents
			.groupBy(col("formid").as("viewformid"))
			.agg(sum("views").as("views"));

		Dataset<Row> userStartedFillingEvents = sparkSession.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "formuserstartedfilling")
			.load();

		userStartedFillingEvents = userStartedFillingEvents.filter(
			"createdate < '" + last5Minutes + "'");

		userStartedFillingEvents = userStartedFillingEvents
			.select("formid").withColumn("started", lit(1));

		userStartedFillingEvents = userStartedFillingEvents
			.groupBy(col("formid").as("startedformid"))
			.agg(sum("started").as("started"));

		Dataset<Row> newViewStartedRows = userViewEvents.join(
			userStartedFillingEvents,
			userViewEvents.col("viewformid").equalTo(
				userStartedFillingEvents.col("startedformid")),
			"left_outer");

		newViewStartedRows = newViewStartedRows.select(
			col("viewformid").as("formid"), col("started"), col("views"));

		Dataset<Row> currentFormUserEvents = sparkSession.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "formsviewsstarted").load();

		currentFormUserEvents =
			currentFormUserEvents.select("formid", "started", "views");

		newViewStartedRows = newViewStartedRows.union(currentFormUserEvents);

		newViewStartedRows = newViewStartedRows
			.groupBy(col("formid"))
			.agg(sum("views").as("views"), sum("started").as("started"));

		newViewStartedRows.write()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "formsviewsstarted")
			.mode(SaveMode.Append)
			.save();
	}

	protected static void runIntermediateTransformation(
		SparkSession sparkSession, String eventId, String destinationTable,
		Date last5Minutes) {

		Dataset<Row> newFormUserEvents = sparkSession.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "analyticsevent")
			.load();

		newFormUserEvents = newFormUserEvents.filter(
			"eventid = '" + eventId +
				"' and createdate > '" + last5Minutes + "'");

		newFormUserEvents = newFormUserEvents.select(
			col("eventproperties").getField("formId").as("formid"),
			col("eventproperties").getField("userId").as("userid"),
			col("createdate"));

		newFormUserEvents = newFormUserEvents
			.groupBy(
				col("formid"), col("userid"))
			.agg(
				min("createdate").as("createdate"));

		newFormUserEvents =
			newFormUserEvents.select("formid", "userid", "createdate");

		Dataset<Row> currentFormUserEvents = sparkSession.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", destinationTable).load();

		currentFormUserEvents =
			currentFormUserEvents.select("formid", "userid", "createdate");

		newFormUserEvents = newFormUserEvents.union(currentFormUserEvents);

		newFormUserEvents.write()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", destinationTable)
			.mode(SaveMode.Append)
			.save();
	}

}
