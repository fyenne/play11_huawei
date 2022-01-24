#!/bin/bash
# pyspark python3 py2 
export PYSPARK_PYTHON="/app/anaconda3/bin/python"
export PYSPARK_DRIVER_PYTHON="/app/anaconda3/bin/python"
spark-submit --master yarn --deploy-mode client --queue root.dsc --conf spark.executor.memoryOverhead=8192 --conf spark.driver.memoryOverhead=8192 --executor-memory 8G --driver-memory 8G ./huawei.py  --start_date ${start_date} --env ${env} --regexp ${regexp} --ou_code ${ou_code} --work_hour_date_range ${work_hour_date_range}