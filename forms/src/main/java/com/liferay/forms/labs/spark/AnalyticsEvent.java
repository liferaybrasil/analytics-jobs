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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author Leonardo Barros
 */
public interface AnalyticsEvent {

	public final String APPLICATION_ID =
		"com.liferay.dynamic.data.mapping.forms.analytics:1.0.0";

	public Dataset<Row> getDataset(
		SparkSession sparkSession, OffsetDateTime referenceDate,
		boolean beforeReferenceDate);

	public Dataset<Row> loadDataset(SparkSession sparkSession);
}
