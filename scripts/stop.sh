#!/bin/bash
export SPARK_WORKER_INSTANCES=4
cd $SPARK_HOME
sbin/stop-slave.sh
sbin/stop-master.sh
