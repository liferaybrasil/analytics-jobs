#!/bin/bash
DIR="$( cd "$( dirname "$0" )" && pwd )"

eval "/usr/lib/spark/bin/spark-submit --master spark://192.168.108.90:7077 --deploy-mode cluster --class com.liferay.workflow.labs.spark.Main $DIR/workflow-process-throughput/target/workflow-process-throughput.jar >> $DIR/workflow-process-throughput.log 2>& 1 &"
eval "/usr/lib/spark/bin/spark-submit --master spark://192.168.108.90:7077 --deploy-mode cluster --class com.liferay.workflow.labs.spark.Main $DIR/workflow-task-throughput/target/workflow-task-throughput.jar >> $DIR/workflow-task-throughput.log 2>& 1 &"
eval "/usr/lib/spark/bin/spark-submit --master spark://192.168.108.90:7077 --deploy-mode cluster --class com.liferay.workflow.labs.spark.Main $DIR/workflow-entities/target/workflow-entities.jar >> $DIR/workflow-entities.log 2>& 1 &"