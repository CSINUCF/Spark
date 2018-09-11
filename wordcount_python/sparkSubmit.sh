#!/usr/bin/env bash
${SPARK_HOME}/bin/spark-submit --master  spark://192.168.35.1:7077 --num-executors 8 --executor-memory 8g  --driver-cores 2 $@
