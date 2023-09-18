
#Count the number of messages in a partition of topic

from kafka import KafkaConsumer, TopicPartition
from json import loads
import sys

bootstrap_servers = ['localhost:9092']
topicname = 'weather4'
partition = 8
count = 0

consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest', value_deserializer =lambda x:loads(x.decode('utf-8')))
consumer.assign([TopicPartition(topicname, partition=partition)])


for message in consumer:
    count = count + 1
    print(count)

