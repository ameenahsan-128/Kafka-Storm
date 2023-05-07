from pymongo import MongoClient
import random
from faker import Faker
from datetime import datetime
import time
from urllib.parse import quote_plus
from pymongo.server_api import ServerApi

fake = Faker()

while True:

    data = {
        "personal_info": {
            'Name': fake.name(),
            'Age': str(random.randint(18, 100)),
            'DOB': str(fake.date_between_dates(date_start=datetime(1950, 1, 1), date_end=datetime(2019, 12, 31)).year),
            'Email': fake.name() + '@' + 'gmail.com',
            'Phone': str(random.choice([8000000000, 9999999999])),
            'signup_at': str(fake.date_time_this_month())
        }
    }

    username = "ameenahsan128"
    password = "Ghfghfghf@123"
    cluster_name = "cluster0"

# #   Escape the special characters in the username and password
#     username = quote_plus(username)
#     password = quote_plus(password)

#     # Create the MongoDB connection string
#     conn_str = f"mongodb+srv://{username}:{password}@{cluster_name}.mongodb.net/mydatabase?retryWrites=true&w=majority"
#     client = MongoClient( conn_str, ServerApi=ServerApi('1') )
   
   
    from pymongo.mongo_client import MongoClient
    conn_str = f"mongodb+srv://{username}:{password}@{cluster_name}.mongodb.net/mydatabase?retryWrites=true&w=majority"
    quote_plus(conn_str)
    # Create a new client and connect to the server
    client = MongoClient(conn_str)
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    
   
   
    db = client['User_Context']
    collection = db['Name']
    collection.insert_one(data)
    client.close()
    time.sleep(10)


