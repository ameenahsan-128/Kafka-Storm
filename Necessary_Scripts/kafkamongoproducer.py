from pymongo import MongoClient
from kafka import KafkaProducer

# MongoDB connection details
mongo_uri = 'localhost'
database_name = 'User_Context'
collection_name = 'Name'

# Kafka connection details
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'base'

# Connect to MongoDB
client = MongoClient(mongo_uri,username='root',password='example')
db = client[database_name]
collection = db[collection_name]

# Connect to Kafka
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

# Retrieve data from MongoDB collection
cursor = collection.find()
for document in cursor:
    # Convert MongoDB document to a string
    message = str(document)
    print(document)

    # Send data to Kafka topic
    producer.send(kafka_topic, message.encode('utf-8'))

# Close Kafka producer
producer.close()

# Close MongoDB connection
client.close()
