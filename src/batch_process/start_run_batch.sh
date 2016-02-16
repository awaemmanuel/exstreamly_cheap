#!/bin/bash

### Run the bash process using the pyspark-cassandra connector on the spark master
spark-submit  --packages TargetHolding/pyspark-cassandra:0.2.4 --conf spark.cassandra.connection.host=172.31.2.39 cleanup_hdfs_files.py
