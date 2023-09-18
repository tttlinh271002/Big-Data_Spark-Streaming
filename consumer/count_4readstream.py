# .\bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 E:\Study\2022_2023_HK2\big_data\code\consumer.py

scala_version = '2.12'
spark_version = '3.4.0'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.4.0'
]

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 pyspark-shell'

from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName("weather") \
        .config("spark.jars.packages", ",".join(packages))\
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, TimestampType, FloatType

schema = StructType([
    StructField("date", StringType(), False),
    StructField("hour", StringType(), False),
    StructField("prcp", StringType(), False),
    StructField("stp", StringType(), False),
    StructField("smax", StringType(), False),
    StructField("smin", StringType(), False),
    StructField("gbrd", StringType(), False),
    StructField("temp", StringType(), False),
    StructField("dewp", StringType(), False),
    StructField("tmax", StringType(), False),
    StructField("tmin", StringType(), False),
    StructField("dmax", StringType(), False),
    StructField("dmin", StringType(), False),
    StructField("hmax", StringType(), False),
    StructField("hmin", StringType(), False),
    StructField("hmdy", StringType(), False),
    StructField("wdct", StringType(), False),
    StructField("gust", StringType(), False),
    StructField("wdsp", StringType(), False),
    StructField("region", StringType(), False),
    StructField("state", StringType(), False),
    StructField("station", StringType(), False),
    StructField("station_code", StringType(), False),
    StructField("latitude", StringType(), False),
    StructField("longitude", StringType(), False),
    StructField("height", StringType(), False),
    StructField("year", StringType(), False)
])

# Kafka readStream
kafka_df1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("assign", """{"weather4":[0,1,2,3,4,5,6,7]}""") \
    .option("startingOffsets", """{"weather4":{"0":-2, "1":-2, "2":-2, "3":-2,  "4":-2, "5":-2, "6":-2, "7":-2}}""") \
    .load() 

kafka_df2 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("assign", """{"weather4":[8,9,10,11,12]}""") \
    .option("startingOffsets", """{"weather4":{"8":-2, "9":-2, "10":-2, "11":-2, "12":-2}}""") \
    .load() 

kafka_df3 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("assign", """{"weather4":[13,14,15,16,17]}""") \
    .option("startingOffsets", """{"weather4":{"13":-2, "14":-2,"15":-2, "16":-2,"17":-2}}""") \
    .load() 

kafka_df4 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("assign", """{"weather4":[18,19,20,21]}""") \
    .option("startingOffsets", """{"weather4":{"18":-2, "19":-2,"20":-2, "21":-2}}""") \
    .load() 

kafka_df1.printSchema()


weatherdf1 = kafka_df1.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
weatherdf2 = kafka_df2.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
weatherdf3 = kafka_df3.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
weatherdf4 = kafka_df4.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

weatherdf1.printSchema()

#Query: Count the number of records per year
from pyspark.sql.functions import *
query_df1 = weatherdf1.groupBy("year").agg(count("date").alias("No_records")).select(col("year"),col("No_Records"))


query_df2 = weatherdf2.groupBy("year").agg(count("date").alias("No_records")).select(col("year"),col("No_Records"))

query_df3 = weatherdf3.groupBy("year").agg(count("date").alias("No_records")).select(col("year"),col("No_Records"))
query_df4 = weatherdf4.groupBy("year").agg(count("date").alias("No_records")).select(col("year"),col("No_Records"))
query_df1.printSchema()

query_df1 = query_df1.selectExpr("CAST(year AS STRING)", 
                             "CAST(No_Records AS STRING)")

query_df2 = query_df2.selectExpr("CAST(year AS STRING)", 
                             "CAST(No_Records AS STRING)")

query_df3 = query_df3.selectExpr("CAST(year AS STRING)", 
                             "CAST(No_Records AS STRING)")

query_df4 = query_df4.selectExpr("CAST(year AS STRING)", 
                             "CAST(No_Records AS STRING)")

#Print to console
write_stream1 = query_df1.writeStream\
        .outputMode("update")\
        .format("console")\
        .start() 


write_stream2 = query_df2.writeStream\
        .outputMode("update")\
        .format("console")\
        .start() 

write_stream3 = query_df3.writeStream\
        .outputMode("update")\
        .format("console")\
        .start() 

write_stream4 = query_df4.writeStream\
        .outputMode("update")\
        .format("console")\
        .start() 

spark.streams.awaitAnyTermination() 
 