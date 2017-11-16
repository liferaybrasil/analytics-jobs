package com.liferay.workflow.labs.spark;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowToTuple;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapTupleToRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.someColumns;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.TimerTask;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;

public class Job extends TimerTask {

    public void run() {
        try {
			doRun();
		}
        catch (AnalysisException e) {
			e.printStackTrace();
		}
    }

	protected void doRun() throws AnalysisException {
		SparkSession spark = SparkSession.builder().master("local")
            .appName("Workflow Throughput")
            .config("spark.cassandra.connection.host", "192.168.108.90")
            .config("spark.cassandra.auth.username", "cassandra")
            .config("spark.cassandra.auth.password", "cassandra")
            .getOrCreate();

        Dataset ds = spark.read().format("org.apache.spark.sql.cassandra")
            .option("table", "analyticsevent").option("keyspace", "analytics")
            .load();

        ds.createTempView("analyticsevent");

		LocalDateTime now = LocalDateTime.now().minus(5, ChronoUnit.MINUTES);

        JavaPairRDD<Long, Long> pairs = ds.sqlContext()
            .sql("select eventid, eventproperties from analyticsevent")
            .filter("eventid = 'KALEO_INSTANCE_COMPLETE' and createdate > '"+ now + "'")
		    .javaRDD()
            .mapToPair(new PairFunction<Row, Long, Long>() {
                @Override
                public Tuple2<Long, Long> call(Row row) throws Exception {
                    Map<String, String> properties = row.getJavaMap(1);

                    return new Tuple2<>(
                        Long.valueOf(
                            properties.get("kaleoDefinitionVersionId")),
                        Long.valueOf(properties.get("duration")));
                }
            });

        Function<Long, AvgCount> createAcc = new Function<Long, AvgCount>() {
            @Override
            public AvgCount call(Long x) {
                return new AvgCount(x, 1);
            }
        };

        Function2<AvgCount, Long, AvgCount> addAndCount = new Function2<AvgCount, Long, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, Long x) {
                a.totalduration += x;
                a.total += 1;
                return a;
            }
        };
        Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, AvgCount b) {
                a.total += b.total;
                a.totalduration += b.totalduration;
                return a;
            }
        };

        JavaRDD<Tuple3<Long, Long, Long>> cassandraRDDTuple = javaFunctions(
            spark.sparkContext())
                .cassandraTable("analytics", "workflowprocessavg",
                    mapRowToTuple(Long.class, Long.class, Long.class))
                .select("kaleodefinitionversionid", "total", "totalduration");

        JavaPairRDD<Long, AvgCount> computationRDDPair = pairs
            .combineByKey(createAcc, addAndCount, combine);

        JavaPairRDD<Long, AvgCount> cassandraRDDPair = cassandraRDDTuple
            .mapToPair(
                new PairFunction<Tuple3<Long, Long, Long>, Long, AvgCount>() {
                    @Override
                    public Tuple2<Long, AvgCount> call(
                        Tuple3<Long, Long, Long> longLongLongTuple3)
                        throws Exception {
                        AvgCount avgCount = new AvgCount(
                            longLongLongTuple3._3(), longLongLongTuple3._2());

                        return new Tuple2<>(longLongLongTuple3._1(), avgCount);
                    }
                });

        JavaRDD<Tuple3<Long, Long, Long>> computationRDDTuple = computationRDDPair
            .map(
                new Function<Tuple2<Long, AvgCount>, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> call(
                        Tuple2<Long, AvgCount> longAvgCountTuple2)
                        throws Exception {
                        return new Tuple3<>(longAvgCountTuple2._1(),
                            longAvgCountTuple2._2().total,
                            longAvgCountTuple2._2().totalduration);
                    }
                });

        JavaRDD<Tuple3<Long, Long, Long>> finalTuple = computationRDDPair
            .leftOuterJoin(cassandraRDDPair).mapValues(
                new Function<Tuple2<AvgCount, Optional<AvgCount>>, AvgCount>() {

                    @Override
                    public AvgCount call(
                        Tuple2<AvgCount, Optional<AvgCount>> avgCountOptionalTuple2)
                        throws Exception {
                        AvgCount count1 = avgCountOptionalTuple2._1();
                        Optional<AvgCount> count2 = avgCountOptionalTuple2._2();

                        if (count2.isPresent()) {
                            count1.total += count2.get().total;
                            count1.totalduration += count2.get().totalduration;
                        }

                        return count1;
                    }
                })
            .map(
                new Function<Tuple2<Long, AvgCount>, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> call(
                        Tuple2<Long, AvgCount> longAvgCountTuple2)
                        throws Exception {
                        return new Tuple3<>(longAvgCountTuple2._1(),
                            longAvgCountTuple2._2().total,
                            longAvgCountTuple2._2().totalduration);
                    }
                });

        javaFunctions(finalTuple)
            .writerBuilder("analytics", "workflowprocessavg",
                mapTupleToRow(Long.class, Long.class, Long.class))
            .withColumnSelector(someColumns("kaleodefinitionversionid", "total",
                "totalduration"))
            .saveToCassandra();
	}

    public static class AvgCount implements java.io.Serializable {
        public AvgCount(long totalduration, long total) {
            this.totalduration = totalduration;
            this.total = total;
        }

        public long getTotalDuration() {
            return total;
        }

        public long getTotal() {
            return totalduration;
        }

        public void setTotalDuration(long total_) {
            this.total = total_;
        }

        public void setTotal(long total_) {
            this.total = total_;
        }

        public Long total;
        public Long totalduration;

        public float avg() {
            return total / (float) totalduration;
        }

        @Override
        public String toString() {
            return "{" + totalduration + "," + total + "}";
        }
    }
}