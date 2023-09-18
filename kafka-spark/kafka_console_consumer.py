#.\bin\windows\kafka-console-consumer.bat --topic test2 --bootstrap-server localhost:9092
scala_version = '2.12'
spark_version = '3.4.0'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.4.0'
]

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, StructField
import pyspark.sql.functions as F

spark = SparkSession \
        .builder \
        .appName("spark_write_to_kafka") \
        .config("spark.jars.packages", ",".join(packages))\
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

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

weather_df = spark.readStream \
                        .schema(schema)\
                        .csv("E:/Study/2022_2023_HK2/big_data/code/smallerCSV/test/")\
                        .withColumn("value", F.to_json( F.struct(F.col("*")) ) )\
                        .withColumn("key", F.lit("key"))\
                        .withColumn("value", F.encode(F.col("value"), "iso-8859-1").cast("binary"))\
                        .withColumn("key", F.encode(F.col("key"), "iso-8859-1").cast("binary"))

# Write the stream to the topic
weather_df\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "temp1")\
    .option("checkpointLocation", "/temp1/checkpoint")\
    .start()\
    .awaitTermination()