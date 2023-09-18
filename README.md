# Real-time Weather Data Processing Project
Simple project with Kafka, Spark structured streaming

## Data
Dataset [here](https://www.kaggle.com/datasets/PROPPG-PPG/hourly-weather-surface-brazil-southeast-region?resource=download)
1. Hourly Climate data from 122 weather stations between 2000 and 2021 of Southeast Brazil.
2. Size: 2.49GB
3. Row: 15 345 216 records
4. Column: 26
Break original data to smaller csv files.



## Send data to Kafka topic

	1. Start Zookeeper, Kafka service.
	2. Create topic “weather” (22 partitions) and send msg to topic via Kafka Producer
    Create 8 producers (shell script file), each producer sends some csv file.

## Read data from Kafka topic 

	1. Consume data from some Kafka topics.
	2. Create a spark streaming application and use readStream function.
	3. Reformat dataframe schema.
    4. Execute any queries and create output sinks for query results

