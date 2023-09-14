from pyspark.sql.functions import year, month, sum, avg
#import matplotlib.pyplot as plt
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from operator import add
#import pandas as pd

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("transaction_1")\
        .getOrCreate()
    
    def good_line(line):
        try:
            fields = line.split(',')
            int(fields[11])
            return True
        except :
            return False
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    

    
    transactions= spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    tr = transactions.filter(good_line)
    month_year = tr.map(lambda b: (time.strftime("%Y-%m", time.gmtime(int(b.split(',')[11]))),1))
    m= month_year.reduceByKey(lambda a, b: a+b)
    m_sorted = m.sortBy(lambda x: x[0])
    count_trans = month_year.count()
    month_year_value = tr.map(lambda b: (time.strftime("%Y-%m", time.gmtime(int(b.split(',')[11]))), float(b.split(',')[7])))
    m_value =  month_year_value.reduceByKey(lambda a, b: a+b)
    month_year_avg = m_value.map(lambda x: (x[0], x[1] / count_trans))
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'transactions' + date_time + '/month_transactions.txt')
    my_result_object.put(Body= json.dumps(m_sorted.collect()))
    my_result_object = my_bucket_resource.Object(s3_bucket,'transactions' + date_time + '/month_avgtransactions.txt')
    my_result_object.put(Body= json.dumps(month_year_avg.collect()))
    spark.stop()