#!/bin/bash
export SPARK_EXECUTOR_INSTANCES=4
bin/spark-shell --master spark://localhost:7077 --packages com.databricks:spark-xml_2.11:0.3.3 --verbose
