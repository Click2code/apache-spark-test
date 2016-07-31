#!/bin/bash
export SPARK_WORKER_INSTANCES=4
sbin/start-master.sh -h localhost -p 7077
sbin/start-slave.sh spark://localhost:7077 -c 1 -m 1G