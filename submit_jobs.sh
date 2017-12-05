#!/bin/bash
DIR="$( cd "$( dirname "$0" )" && pwd )"

eval "/usr/lib/spark/bin/spark-submit --master spark://192.168.108.90:7077 --deploy-mode cluster --class com.liferay.workflow.labs.spark.Main $DIR/workflow/target/workflow.jar >> $DIR/workflow.log 2>& 1 &"
