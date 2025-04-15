#!/bin/bash

default_path_input="/index/data"
input_path="/data"
path_temporary_1="/tmp/index/phase1"
path_temporary_2="/tmp/index/phase2"
path_to_mapper_1="mapreduce/mapper1.py"
path_to_reducer_1="mapreduce/reducer1.py"
path_to_mapper_2="mapreduce/mapper2.py"
path_to_reducer_2="mapreduce/reducer2.py"

if [ $# -eq 1 ]; then
    CUSTOM_PATH=$1
    if [[ $CUSTOM_PATH != hdfs://* ]]; then
        echo "Uploading local file/directory $CUSTOM_PATH to HDFS"
        TEMP_INPUT="/tmp/index_input"
        hdfs dfs -rm -r -f $TEMP_INPUT
        hdfs dfs -mkdir -p $TEMP_INPUT
        hdfs dfs -put $CUSTOM_PATH $TEMP_INPUT/
        input_path=$TEMP_INPUT
    else
        input_path=$CUSTOM_PATH
    fi
    echo "Using input path: $input_path"
fi

for script in $path_to_mapper_1 $path_to_reducer_1 $path_to_mapper_2 $path_to_reducer_2; do
    if [ ! -f "$script" ]; then
        echo "Error: Script $script not found"
        exit 1
    fi
    chmod +x $script
done

hdfs dfs -test -e $input_path

if [ $? -ne 0 ]; then
    echo "Error: Input path $input_path does not exist in HDFS"
    exit 1
fi


hdfs dfs -rm -r -f $path_temporary_1
hdfs dfs -rm -r -f $path_temporary_2

echo "Starting first MapReduce job: Term extraction"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files $path_to_mapper_1,$path_to_reducer_1 \
    -mapper "python3 $(basename $path_to_mapper_1)" \
    -reducer "python3 $(basename $path_to_reducer_1)" \
    -input "$default_path_input" \
    -output $path_temporary_1
    # -D mapreduce.job.reduces=3 \

if [ $? -ne 0 ]; then
    echo "Error: First MapReduce job failed"
    exit 1
fi

echo "Starting second MapReduce job: Building inverted index"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files $path_to_mapper_2,$path_to_reducer_2 \
    -mapper "python3 $(basename $path_to_mapper_2)" \
    -reducer "python3 $(basename $path_to_reducer_2)" \
    -input "$path_temporary_1/part-*" \
    -output $path_temporary_2
    # -D mapreduce.job.reduces=3 \

if [ $? -ne 0 ]; then
    echo "Error: Second MapReduce job failed"
    exit 1
fi

echo "Storing index data in Cassandra..."
python3 app.py --index_path $path_temporary_2

if [ $? -eq 0 ]; then
    echo "Indexing completed successfully. Data has been stored in Cassandra."
else
    echo "Error: Failed to store index data in Cassandra"
    exit 1
fi