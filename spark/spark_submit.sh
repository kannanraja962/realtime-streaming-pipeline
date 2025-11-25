#!/bin/bash

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0 \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  spark/streaming_job.py
