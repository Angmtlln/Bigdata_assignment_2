#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"
source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python
spark-submit --master yarn --archives /app/.venv.tar.gz#.venv query.py  $1