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

import java.time.OffsetDateTime;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author Leonardo Barros
 */
public class AnalyticsDatasetImpl implements AnalyticsDataset {

	@Override
	public Dataset<Row> getDataset(
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

	@Override
	public Dataset<Row> loadDataset(SparkSession sparkSession) {
		return sparkSession.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "analytics")
			.option("table", "analyticsevent")
			.load();
	}
}
