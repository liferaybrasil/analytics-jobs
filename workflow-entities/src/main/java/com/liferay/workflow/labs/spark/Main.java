package com.liferay.workflow.labs.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Main {

	public static void main(String[] args) {
		doRun();
	}

	protected static void doRun() {
		SparkSession spark = SparkSession.builder().appName("Workflow Entities")
			.config("spark.cassandra.connection.host", "192.168.108.90")
			.config("spark.cassandra.auth.username", "cassandra")
			.config("spark.cassandra.auth.password", "cassandra").getOrCreate();

		Dataset<Row> analyticsEventDataSet = spark.read()
			.format("org.apache.spark.sql.cassandra")
			.options(_analyticsEventOptions).load();

		analyticsEventDataSet = analyticsEventDataSet.filter("eventid = '"
			+ _eventId + "' and createdate > '" + _last5Minutes + "'");

		analyticsEventDataSet = analyticsEventDataSet
			.select(col("eventproperties").getField("kaleoDefinitionVersionId")
				.as("id"), col("eventproperties").getField("name").as("name"));

		analyticsEventDataSet = analyticsEventDataSet.withColumn("entity",
			lit("KALEO_DEFINITION_VERSION"));

		analyticsEventDataSet.write().format("org.apache.spark.sql.cassandra")
			.options(_workflowEntitiesOptions).mode(SaveMode.Append).save();

		spark.stop();
	}

	private static final Map<String, String> _analyticsEventOptions = new HashMap<String, String>() {
		{
			put("keyspace", "analytics");
			put("table", "analyticsevent");
		}
	};

	private static final String _eventId = "KALEO_DEFINITION_VERSION_CREATE";

	private static final Date _last5Minutes = Date
		.from(Instant.now().minus(5, ChronoUnit.MINUTES));

	private static final Map<String, String> _workflowEntitiesOptions = new HashMap<String, String>() {
		{
			put("keyspace", "analytics");
			put("table", "workflowentities");
		}
	};
}