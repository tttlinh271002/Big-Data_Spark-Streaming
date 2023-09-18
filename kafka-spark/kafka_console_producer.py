# .\bin\windows\kafka-console-producer.bat --bootstrap-server localhost:9092 --topic test1 --property "parse.key=true" --property "key.separator=:"

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
        .appName("kafka_console_producer") \
        .config("spark.jars.packages", ",".join(packages))\
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test1") \
    .option("startingOffsets", "earliest") \
    .load() 

write_stream = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .start()
write_stream.awaitTermination()

