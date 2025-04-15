#!/bin/bash
source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

hdfs dfs -put -f a.parquet / 
hdfs dfs -rm -f /data/*

spark-submit --conf spark.executor.memory=10G --conf spark.driver.memory=10G prepare_data.py 
echo "Put data to hdfs"
hdfs dfs -put data / 


echo "Printing data content in hdfs"
hdfs dfs -ls /data 

echo "Printing data content in index/data"
hdfs dfs -ls /index/data 

echo "Finished preparing data"