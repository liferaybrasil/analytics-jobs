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
			.appName("Workflow Task Throughput")
			.config("spark.cassandra.connection.host", "192.168.108.90")
			.config("spark.cassandra.auth.username", "cassandra")
			.config("spark.cassandra.auth.password", "cassandra").getOrCreate();

		Dataset<Row> workflowTaskAvgDataSet = spark.read()
			.format("org.apache.spark.sql.cassandra")
			.options(_workflowTaskAvgOptions).load();

		Dataset<Row> analyticsEventDataSet1 = spark.read()
			.format("org.apache.spark.sql.cassandra")
			.options(_analyticsEventOptions).load();

		analyticsEventDataSet1 = analyticsEventDataSet1.filter("eventid = '"
			+ _eventId1 + "' and createdate > '" + _last5Minutes + "'");

		Dataset<Row> analyticsEventDataSet2 = spark.read()
			.format("org.apache.spark.sql.cassandra")
			.options(_analyticsEventOptions).load();

		analyticsEventDataSet2 = analyticsEventDataSet2.filter("eventid = '"
			+ _eventId2 + "' and createdate > '" + _last5Minutes + "'");

		Dataset<Row> analyticsEventDataSet = analyticsEventDataSet1
			.union(analyticsEventDataSet2);

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("eventproperties").getField("kaleoDefinitionVersionId")
				.as("kaleodefinitionversionid"),
			col("eventproperties").getField("kaleoTaskId").as("kaleotaskid"),
			col("eventproperties").getField("duration").as("totalduration"),
			col("eventproperties").getField("assigneeClassName")
				.as("classname"),
			col("eventproperties").getField("assigneeClassPK").as("classpk"));

		analyticsEventDataSet = analyticsEventDataSet.withColumn("total",
			lit(1));

		analyticsEventDataSet = analyticsEventDataSet.select(
			"kaleodefinitionversionid", "kaleotaskid", "classname", "classpk",
			"total", "totalduration");

		workflowTaskAvgDataSet = workflowTaskAvgDataSet.select(
			"kaleodefinitionversionid", "kaleotaskid", "classname", "classpk",
			"total", "totalduration");

		workflowTaskAvgDataSet = workflowTaskAvgDataSet
			.union(analyticsEventDataSet);

		workflowTaskAvgDataSet = workflowTaskAvgDataSet
			.groupBy("kaleodefinitionversionid", "kaleotaskid", "classname",
				"classpk")
			.agg(sum("totalduration").as("totalduration"),
				sum("total").as("total"));

		workflowTaskAvgDataSet.write().format("org.apache.spark.sql.cassandra")
			.options(_workflowTaskAvgOptions).mode(SaveMode.Append).save();

		spark.stop();
	}

	private static final Map<String, String> _analyticsEventOptions = new HashMap<String, String>() {
		{
			put("keyspace", "analytics");
			put("table", "analyticsevent");
		}
	};

	private static final String _eventId1 = "KALEO_TASK_ASSIGNMENT_INSTANCE_COMPLETE";

	private static final String _eventId2 = "KALEO_TASK_ASSIGNMENT_INSTANCE_DELETE";

	private static final Date _last5Minutes = Date
		.from(Instant.now().minus(5, ChronoUnit.MINUTES));

	private static final Map<String, String> _workflowTaskAvgOptions = new HashMap<String, String>() {
		{
			put("keyspace", "analytics");
			put("table", "workflowtaskavg");
		}
	};
}