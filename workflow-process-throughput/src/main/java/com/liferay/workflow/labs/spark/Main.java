
package com.liferay.workflow.labs.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author Inácio Nery
 */
public class Main {

	public static void main(String[] args) {

		SparkSession spark =
			SparkSession.builder().appName("Workflow Throughput").config(
				"spark.cassandra.connection.host", "192.168.108.90").config(
					"spark.cassandra.auth.username", "cassandra").config(
						"spark.cassandra.auth.password",
						"cassandra").getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> analyticsEventDataSet =
			spark.read().format("org.apache.spark.sql.cassandra").options(
				_analyticsEventOptions).load();

		Dataset<Row> workflowProcessAvgNewDataSet =
			kaleoInstanceComplete(analyticsEventDataSet);

		workflowProcessAvgNewDataSet =
			kaleoInstanceCreate(analyticsEventDataSet).union(
				workflowProcessAvgNewDataSet);

		workflowProcessAvgNewDataSet =
			kaleoInstanceRemove(analyticsEventDataSet).union(
				workflowProcessAvgNewDataSet);

		workflowProcessAvgNewDataSet =
			filterAndGroupBy(spark, workflowProcessAvgNewDataSet);

		workflowProcessAvgNewDataSet.write().format(
			"org.apache.spark.sql.cassandra").options(
				_workflowProcessAvgOptions).mode(SaveMode.Append).save();

		spark.stop();
	}

	protected static Dataset<Row> kaleoInstanceComplete(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet = analyticsEventDataSet.filter(
			"eventid = 'KALEO_INSTANCE_COMPLETE' and createdate > '" +
				_last5Minutes + "'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionVersionId").as(
				"processversionid"),
			col("eventproperties").getField("date").cast("date").as("date"),
			col("eventproperties").getField("duration").as("totalduration"));

		analyticsEventDataSet = analyticsEventDataSet.withColumn(
			"totalcompleted", lit(1)).withColumn(
				"totalremoved", lit(0)).withColumn("total", lit(0));

		return analyticsEventDataSet.select(
			"analyticskey", "processversionid", "date", "total",
			"totalcompleted", "totalduration", "totalremoved");
	}

	protected static Dataset<Row> kaleoInstanceCreate(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet = analyticsEventDataSet.filter(
			"eventid = 'KALEO_INSTANCE_CREATE' and createdate > '" +
				_last5Minutes + "'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionVersionId").as(
				"processversionid"),
			col("eventproperties").getField("date").cast("date").as("date"));

		analyticsEventDataSet = analyticsEventDataSet.withColumn(
			"totalduration", lit(0)).withColumn(
				"totalcompleted", lit(0)).withColumn(
					"totalremoved", lit(0)).withColumn("total", lit(1));

		return analyticsEventDataSet.select(
			"analyticskey", "processversionid", "date", "total",
			"totalcompleted", "totalduration", "totalremoved");
	}

	protected static Dataset<Row> kaleoInstanceRemove(
		Dataset<Row> analyticsEventDataSet) {

		analyticsEventDataSet = analyticsEventDataSet.filter(
			"eventid = 'KALEO_INSTANCE_REMOVE' and createdate > '" +
				_last5Minutes + "'");

		analyticsEventDataSet = analyticsEventDataSet.select(
			col("analyticskey"),
			col("eventproperties").getField("kaleoDefinitionVersionId").as(
				"processversionid"),
			col("eventproperties").getField("date").cast("date").as("date"));

		analyticsEventDataSet = analyticsEventDataSet.withColumn(
			"totalduration", lit(0)).withColumn(
				"totalcompleted", lit(0)).withColumn(
					"totalremoved", lit(1)).withColumn("total", lit(0));

		return analyticsEventDataSet.select(
			"analyticskey", "processversionid", "date", "total",
			"totalcompleted", "totalduration", "totalremoved");
	}

	protected static Dataset<Row> filterAndGroupBy(
		SparkSession spark, Dataset<Row> workflowProcessAvgNewDataSet) {

		Dataset<Row> workflowProcessAvgExistDataSet =
			spark.read().format("org.apache.spark.sql.cassandra").options(
				_workflowProcessAvgOptions).load();

		workflowProcessAvgExistDataSet = workflowProcessAvgExistDataSet.filter(
			col("analyticskey").isin(
				workflowProcessAvgNewDataSet.select("analyticskey").columns()));

		workflowProcessAvgExistDataSet = workflowProcessAvgExistDataSet.filter(
			col("processversionid").isin(
				workflowProcessAvgNewDataSet.select(
					"processversionid").columns()));

		workflowProcessAvgExistDataSet = workflowProcessAvgExistDataSet.filter(
			col("date").isin(
				workflowProcessAvgNewDataSet.select("date").columns()));

		workflowProcessAvgExistDataSet = workflowProcessAvgExistDataSet.select(
			"analyticskey", "processversionid", "date", "total",
			"totalcompleted", "totalduration", "totalremoved");

		workflowProcessAvgNewDataSet =
			workflowProcessAvgNewDataSet.union(workflowProcessAvgExistDataSet);

		return workflowProcessAvgNewDataSet.groupBy(
			"analyticskey", "processversionid", "date").agg(
				sum("total").as("total"),
				sum("totalcompleted").as("totalcompleted"),
				sum("totalremoved").as("totalremoved"),
				sum("totalduration").as("totalduration"));
	}

	private static final Map<String, String> _analyticsEventOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "analyticsevent");
			}
		};

	private static final String _last5Minutes =
		OffsetDateTime.now().minusMinutes(5).toString();

	private static final Map<String, String> _workflowProcessAvgOptions =
		new HashMap<String, String>() {

			{
				put("keyspace", "analytics");
				put("table", "workflowprocessavg");
			}
		};
}
