#!/bin/bash

# Set Spark configuration
export SPARK_DRIVER_MEMORY=4g

# Run job
/opt/spark/spark-3.5.3-bin-hadoop3/bin/spark-submit \
    --driver-memory $SPARK_DRIVER_MEMORY \
    --packages org.apache.hadoop:hadoop-aws:3.3.1 \
    --packages org.apache.hadoop:hadoop-common:3.3.1\
    src/reconciliation_job.py