
# Ingest data from CSV file to Kafka topic 
from kafka import KafkaProducer
from csv import DictReader
import json
import sys
import time

input_file = sys.argv[1]
# input_file = "smallerCSV/2002.csv"
bootstrap_servers = ['localhost:9092']
topicname = 'weather4'

start = time.time()
print("Starting import file" + input_file + " " + str(start))
part = 0

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
producer = KafkaProducer()

with open(input_file, "r", encoding="utf-8") as new_obj:
    csv_dict_reader = DictReader(new_obj)
    for row in csv_dict_reader:
        part = int(row['year']) % 1000
        producer.send(topicname, json.dumps(row).encode('utf-8'), partition=part)

end = time.time()
total_time = end-start
print("Finishing import file " + input_file + " " + str(end))
print("Total time " + str(total_time) + "\n")
