#!/bin/bash
# pyspark python3 py2 
export PYSPARK_PYTHON="/app/anaconda3/bin/python"
export PYSPARK_DRIVER_PYTHON="/app/anaconda3/bin/python"
spark-submit --master yarn --deploy-mode client --queue root.dsc --conf spark.executor.memoryOverhead=1536 --conf spark.driver.memoryOverhead=1536 --executor-memory 4G --driver-memory 4G ./huawei.py  --start_date ${start_date}