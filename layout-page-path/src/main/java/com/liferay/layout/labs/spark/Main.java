package com.liferay.layout.labs.spark;

import java.util.Map;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

	public static void main(String[] args) {
		try {
			doRun();
		} catch (AnalysisException e) {
			e.printStackTrace();
		}
	}

	protected static void doRun() throws AnalysisException {
		SparkSession sparkSession = SparkSession.builder().master("local[*]")
				.appName("Layout Page Path")
				.config("spark.cassandra.connection.host", "127.0.0.1")
				.getOrCreate();

		Dataset<Row> dataSet = sparkSession.read()
				.format("org.apache.spark.sql.cassandra")
				.option("keyspace", "analytics")
				.option("table", "analyticsevent").load();

		dataSet.createTempView("events");

		Dataset<Path> pathDataSet =
				dataSet.sqlContext()
					   .sql("select context, eventproperties from events")
					   .map((row) -> {
								Map<Object, Object> context = row.getJavaMap(0);
								Map<Object, Object> properties =
									row.getJavaMap(1);

								return new Path(
									(String) properties.get("referrer"),
									(String) context.get("url"));
							},
							Encoders.bean(Path.class));

		pathDataSet.createTempView("paths");

		Dataset<Row> outputDataSet =
			pathDataSet.sqlContext()
				   	   .sql("select source, destination, count(destination) " +
						   "as total from paths group by source, destination " +
						   		"order by total desc");


		outputDataSet.write()
					 .format("org.apache.spark.sql.cassandra")
					 .option("keyspace", "analytics")
					 .option("table", "pagepaths")
					 .save();
	}

	public static class Path implements java.io.Serializable {

		public Path(String source, String destination) {
			this.source = source.trim();
			this.destination = destination.trim();
		}

		public String getSource() {
			return source;
		}

		public String getDestination() {
			return destination;
		}

		public void setSource(String source) {
			this.source = source;
		}

		public void setDestination(String destination) {
			this.destination = destination;
		}

		public String source;
		public String destination;

		@Override
		public String toString() {
			return "{" + source + ", " + destination + "}";
		}
	}
}