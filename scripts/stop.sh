#!/bin/bash
export SPARK_WORKER_INSTANCES=4
sbin/stop-slave.sh
sbin/stop-master.sh
