from faker import Faker
from kafka import KafkaProducer
import time
fake=Faker()
import random
import json
from datetime import datetime


bootstrap_servers = ['localhost:9092']
topicName = 'base'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()

while True:

    data={
        "personal_info": {
            'Name': fake.name(),
            'Age': str(random.randint(18, 100)),
            'DOB': str(fake.date_between_dates(date_start=datetime(1950, 1, 1), date_end=datetime(2019, 12, 31)).year),
            'Email': fake.name() + '@' + 'gmail.com',
            'Phone': str(random.choice([8000000000, 9999999999])),
            'signup_at': str(fake.date_time_this_month())
        }
    }
    m=json.dumps(data)
    ack=producer.send(topicName, m.encode('utf-8'))
    metadata = ack.get()

    print(metadata.topic)
    print(metadata.partition)

    producer.flush()
    time.sleep(20)
    
