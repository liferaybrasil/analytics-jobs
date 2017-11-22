package com.liferay.workflow.labs.spark;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapTupleToRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.someColumns;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.AnalysisException;

import com.datastax.spark.connector.japi.CassandraRow;

import scala.Tuple2;
import scala.Tuple3;

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
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Workflow Entities");
        sparkConf.set("spark.cassandra.connection.host", "192.168.108.90");
        sparkConf.set("spark.cassandra.auth.username", "cassandra");
        sparkConf.set("spark.cassandra.auth.password", "cassandra");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Instant last5Minutes = Instant.now().minus(5, ChronoUnit.MINUTES);

        JavaPairRDD<Long, String> definitionNamePair = javaFunctions(
            sparkContext).cassandraTable("analytics", "analyticsevent")
                .select("eventid", "eventproperties")
                .where("eventid = ? and createdate > ?",
                    "KALEO_DEFINITION_VERSION_CREATE",
                    new Date().from(last5Minutes))
                .mapToPair(new PairFunction<CassandraRow, Long, String>() {

                    @Override
                    public Tuple2<Long, String> call(CassandraRow cassandraRow)
                        throws Exception {
                        Map<Object, Object> eventproperties = cassandraRow
                            .getMap("eventproperties");
                        return new Tuple2<>(
                            Long.valueOf((String) eventproperties
                                .get("kaleoDefinitionVersionId")),
                            (String) eventproperties.get("name"));
                    }
                });

        JavaRDD<Tuple3<String, Long, String>> workflowEntitiesTuple3 = definitionNamePair
            .map(
                new Function<Tuple2<Long, String>, Tuple3<String, Long, String>>() {
                    @Override
                    public Tuple3<String, Long, String> call(
                        Tuple2<Long, String> longStringTuple2)
                        throws Exception {
                        return new Tuple3<>("KALEO_DEFINITION_VERSION",
                            longStringTuple2._1(), longStringTuple2._2());
                    }
                });

        javaFunctions(workflowEntitiesTuple3)
            .writerBuilder("analytics", "workflowentities",
                mapTupleToRow(String.class, Long.class, String.class))
            .withColumnSelector(someColumns("entity", "id", "name"))
            .saveToCassandra();
    }
}