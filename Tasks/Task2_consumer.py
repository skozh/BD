# Import Libraries
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import json
import time
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaConsumer, TopicPartition
import logging

s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)

# Configuration
KAFKA_HOSTS = 'localhost:9092'
KAFKA_VERSION = (0, 10, 2)
topics = 'posts'


# Test Message
consumer = KafkaConsumer('test', bootstrap_servers=KAFKA_HOSTS, group_id=None, auto_offset_reset ='earliest')
tp = TopicPartition('test',0)
print("Kafka Test Message")
for message in consumer:
    print(message)
    if message.offset == consumer.position(tp) - 1:
        break


# Spark
spark = SparkSession.builder.master("local[*]") \
    .appName("Stream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


# Read Stream
lines = spark\
  .readStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", KAFKA_HOSTS)\
  .option("subscribe", topics)\
  .option("startingOffsets", "latest")\
  .load()

# Split the lines into words
words = lines.select( lines.timestamp, 
       split(lines.value, " ").alias("word")
)

# Generate running word count
print("Word count: ")
finaldf = words.withColumn('post_id', regexp_extract(words["word"].getItem(1), "\\d+", 0))\
                    .withColumn('sex', regexp_extract(words["word"].getItem(3), "\\d+", 0))\
                    .withColumn('age', regexp_extract(words["word"].getItem(5), "\\d+", 0))\
                    .withColumn('totalwords', regexp_extract(words["word"].getItem(7), "\\d+", 0))\
                    .withColumn('withoutstopwords', regexp_extract(words["word"].getItem(9), "\\d+", 0))\
                    .select('post_id', 'sex', 'age', 'totalwords', 'withoutstopwords', 'timestamp')


def func(age):
    if age < 18:
        return '<18'
    elif age >=18 and age<27:
        return '18-27'
    elif age >=27 and age <44:
        return '27-44'
    elif age>=44 and age<60:
        return '44-60'
    else:
        return '>=60'

func_udf = udf(func, StringType())

sexCounts = finaldf.groupby(window(col('timestamp'), "10 minutes", "5 minutes"),'sex').count()
ageCounts = finaldf.withColumn('age_category', func_udf(finaldf['age'].cast(IntegerType())))\
                    .groupby(window(col('timestamp'), "10 minutes", "5 minutes"),'age_category').count()

wordCounts = finaldf.select('timestamp', col('totalwords')\
                    .cast(IntegerType()), col('withoutstopwords')\
                    .cast(IntegerType())).groupBy(window(col('timestamp'), "10 minutes", "5 minutes")).sum()

 # Start running the query that prints the running counts to the console
query = ageCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query = sexCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()