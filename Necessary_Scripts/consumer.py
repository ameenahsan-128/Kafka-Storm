from kafka import KafkaConsumer
import sys



bootstrap_servers = ['localhost:9092']
topicName = 'test'
consumer = KafkaConsumer (topicName, group_id = 'group1',bootstrap_servers = bootstrap_servers,
auto_offset_reset = 'earliest')


try:
    for message in consumer:
        print (message)
except KeyboardInterrupt:
    sys.exit()