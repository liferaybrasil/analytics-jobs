package com.liferay.workflow.labs.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {
        try {
            doRun();
        }
        catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    protected static void doRun() throws AnalysisException {
        SparkSession spark = SparkSession.builder()
            .appName("Workflow Throughput")
            .config("spark.cassandra.connection.host", "192.168.108.90")
            .config("spark.cassandra.auth.username", "cassandra")
            .config("spark.cassandra.auth.password", "cassandra")
            .master("local").getOrCreate();

        Dataset<Row> analyticsEventDataSet = spark.read()
            .format("org.apache.spark.sql.cassandra")
            .options(new HashMap<String, String>() {
                {
                    put("keyspace", "analytics");
                    put("table", "analyticsevent");
                }
            }).load();

        Instant last5Minutes = Instant.now().minus(5, ChronoUnit.MINUTES);

        analyticsEventDataSet = analyticsEventDataSet
            .filter("eventid = 'KALEO_INSTANCE_COMPLETE' and createdate < '"
                + new Date().from(last5Minutes) + "'");

        analyticsEventDataSet = analyticsEventDataSet.select(
            col("eventproperties").getField("kaleoDefinitionVersionId")
                .as("kaleodefinitionversionid"),
            col("eventproperties").getField("duration").as("totalduration"));

        analyticsEventDataSet = analyticsEventDataSet.withColumn("total",
            lit(1));

        analyticsEventDataSet = analyticsEventDataSet
            .select("kaleoDefinitionVersionId", "total", "totalduration");

        Dataset<Row> workflowProcessAvgDataSet = spark.read()
            .format("org.apache.spark.sql.cassandra")
            .options(new HashMap<String, String>() {
                {
                    put("keyspace", "analytics");
                    put("table", "workflowprocessavg");
                }
            }).load();

        workflowProcessAvgDataSet = workflowProcessAvgDataSet
            .select("kaleoDefinitionVersionId", "total", "totalduration");

        workflowProcessAvgDataSet = workflowProcessAvgDataSet
            .union(analyticsEventDataSet);

        workflowProcessAvgDataSet = workflowProcessAvgDataSet
            .groupBy(col("kaleodefinitionversionid"))
            .agg(sum("totalduration").as("totalduration"),
                sum("total").as("total"));

        workflowProcessAvgDataSet.write()
            .format("org.apache.spark.sql.cassandra")
            .options(new HashMap<String, String>() {
                {
                    put("keyspace", "analytics");
                    put("table", "workflowprocessavg");
                }
            }).mode(SaveMode.Append).save();

        spark.stop();
    }
}