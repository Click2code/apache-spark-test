#!/bin/bash
export SPARK_EXECUTOR_INSTANCES=4
bin/spark-shell --master spark://localhost:7077 --packages com.databricks:spark-csv_2.11:1.4.0 --verbose
