#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
cd $DIR


#exec 2>>`basename $0``date +"%Y%m%d"`.log

JARS=$(find "$DIR/lib/" -name '*.jar' | xargs echo | tr ' ' ',')

HADOOP_USER_NAME=spark  /usr/hdp/current/spark2-client/bin/spark-submit \
  --class "com.rao2100.basicspark.SpringSampleApplication"  \
  --conf spark.ui.port=4041 \
  --num-executors 1 \
  --total-executor-cores 8 \
  --executor-memory 2G \
  --driver-memory 3G \
  --master local \
  /tmp/BasicSpark-1.0-SNAPSHOT.jar --spring.config.location=/tmp/application.yml
