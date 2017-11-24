package com.liferay.workflow.labs.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

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
		SparkSession spark = SparkSession.builder()
			.appName("Workflow Throughput")
			.config("spark.cassandra.connection.host", "192.168.108.90")
			.config("spark.cassandra.auth.username", "cassandra")
			.config("spark.cassandra.auth.password", "cassandra").getOrCreate();

		Dataset<Row> workflowProcessAvgDataSet = spark.read()
			.format("org.apache.spark.sql.cassandra")
			.options(_workflowProcessAvgOptions).load();

		Dataset<Row> analyticsEventDataSet = spark.read()
			.format("org.apache.spark.sql.cassandra")
			.options(_analyticsEventOptions).load();

		analyticsEventDataSet = analyticsEventDataSet.filter("eventid = '"
			+ _eventId + "' and createdate > '" + _last5Minutes + "'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("eventproperties").getField("kaleoDefinitionVersionId")
				.as("kaleodefinitionversionid"),
			col("eventproperties").getField("duration").as("totalduration"));

		analyticsEventDataSet = analyticsEventDataSet.withColumn("total",
			lit(1));

		analyticsEventDataSet = analyticsEventDataSet
			.select("kaleodefinitionversionid", "total", "totalduration");

		workflowProcessAvgDataSet = workflowProcessAvgDataSet
			.select("kaleodefinitionversionid", "total", "totalduration");

		workflowProcessAvgDataSet = workflowProcessAvgDataSet
			.union(analyticsEventDataSet);

		workflowProcessAvgDataSet = workflowProcessAvgDataSet
			.groupBy(col("kaleodefinitionversionid"))
			.agg(sum("totalduration").as("totalduration"),
				sum("total").as("total"));

		workflowProcessAvgDataSet.write()
			.format("org.apache.spark.sql.cassandra")
			.options(_workflowProcessAvgOptions).mode(SaveMode.Append).save();

		spark.stop();
	}

	private static final Map<String, String> _analyticsEventOptions = new HashMap<String, String>() {
		{
			put("keyspace", "analytics");
			put("table", "analyticsevent");
		}
	};

	private static final String _eventId = "KALEO_INSTANCE_COMPLETE";

	private static final Date _last5Minutes = Date
		.from(Instant.now().minus(5, ChronoUnit.MINUTES));

	private static final Map<String, String> _workflowProcessAvgOptions = new HashMap<String, String>() {
		{
			put("keyspace", "analytics");
			put("table", "workflowprocessavg");
		}
	};
}