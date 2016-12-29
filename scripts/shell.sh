#!/bin/bash
export SPARK_EXECUTOR_INSTANCES=4
cd $SPARK_HOME
bin/spark-shell --master spark://localhost:7077 --verbose
