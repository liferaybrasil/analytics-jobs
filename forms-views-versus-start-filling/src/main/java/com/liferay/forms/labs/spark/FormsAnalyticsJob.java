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

import java.time.OffsetDateTime;

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

		AnalyticsDataset analyticsDataset = new AnalyticsDatasetImpl();

		FormsAnalyticsHelper formsAnalyticsHelper = 
			new FormsAnalyticsHelper(sparkSession, analyticsDataset);

		formsAnalyticsHelper.run(referenceDate);

		FormFieldsAnalyticsHelper formFieldsAnalyticsHelper =
			new FormFieldsAnalyticsHelper(analyticsDataset, sparkSession);

		formFieldsAnalyticsHelper.run(referenceDate);

		sparkSession.stop();
	}
}
