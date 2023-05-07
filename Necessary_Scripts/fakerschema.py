from faker import Faker
import random
import json
from datetime import datetime

fake = Faker()


data = {
    "personal_info": {
        'Name': fake.name(),
        'Age': str(random.randint(18, 100)),
        'DOB': fake.date_of_birth(minimum_age=18, maximum_age=100).strftime("%m/%d/%Y"),
        'Email': fake.name() + '@' + 'gmail.com',
        'Phone': str(random.randint(8000000000, 9999999999)),
    }
}

print(json.dumps(data, indent=4))
