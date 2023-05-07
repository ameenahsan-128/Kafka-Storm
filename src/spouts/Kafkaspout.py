#class KafkaSpout(Spout):
#     outputs = ['message']

#     def initialize(self, stormconf, context):
#         self.consumer = KafkaConsumer(
#             'base',
#             bootstrap_servers='localhost:9092',
#             group_id='group1',
#             auto_offset_reset='earliest')

#     def next_tuple(self):
#         for message in self.consumer:
#             self.emit([message.value.decode('utf-8')])
#             # self.log(message)

from streamparse import Spout
from kafka import KafkaConsumer, errors

# class KafkaSpout(Spout):
#     outputs = ['message']

#     def initialize(self, stormconf, context):
#         self.consumer = KafkaConsumer(
#             'base',
#             bootstrap_servers='localhost:9092',
#             group_id='group1',
#             auto_offset_reset='earliest')
#         self.max_retries = 3  # maximum number of retries
#         self.retry_interval = 5  # retry interval in seconds
#         self.current_retry = 0  # current retry attempt

#     def next_tuple(self):
#         while True:
#             try:
#                 message = self.consumer.poll(timeout_ms=1000, max_records=1)
#                 if not message:
#                     continue
#                 for tp, messages in message.items():
#                     for msg in messages:
#                         self.emit([msg.value.decode('utf-8')])
#                         # self.log(msg)
#                 self.consumer.commit()
#             except errors.KafkaError as e:
#                 self.log('Error: {}'.format(e))
#                 if self.current_retry < self.max_retries:
#                     self.current_retry += 1
#                     self.log('Retrying in {} seconds...'.format(self.retry_interval))
#                     self.sleep(self.retry_interval)
#                 else:
#                     self.log('Maximum retries reached. Exiting...')
#                     self.fail(e)
#                     break

#     def ack(self, tup_id):
#         pass

#     def fail(self, tup_id):
#         self.log('Failed to process tuple: {}'.format(tup_id))
#         # Handle failure of tuple processing here, if needed


from datetime import datetime, timedelta
from collections import deque

# class KafkaSpout(Spout):
#     outputs = ['message']

#     def initialize(self, stormconf, context):
#         self.consumer = KafkaConsumer(
#             'base',
#             bootstrap_servers='localhost:9092',
#             group_id='group1',
#             auto_offset_reset='earliest')
#         self.max_retries = 3  # maximum number of retries
#         self.retry_interval = 5  # retry interval in seconds
#         self.current_retry = 0  # current retry attempt
#         self.tick_interval = 10  # tick interval in seconds
#         self.last_tick_time = datetime.now()
#         self.tick_queue = deque()

#     def next_tuple(self):
#         if len(self.tick_queue) > 0:
#             tick_tuple = self.tick_queue.popleft()
#             self.emit(tick_tuple)
#             return

#         try:
#             message = self.consumer.poll(timeout_ms=1000, max_records=1)
#             if not message:
#                 # Emit a tick tuple if there's no message
#                 if (datetime.now() - self.last_tick_time).seconds > self.tick_interval:
#                     self.tick_queue.append(([None],))
#                     self.last_tick_time = datetime.now()
#                 return

#             for tp, messages in message.items():
#                 for msg in messages:
#                     self.emit([msg.value.decode('utf-8')])

#                     # self.log(msg)
#             self.consumer.commit()

#         except errors.KafkaError as e:
#             self.log('Error: {}'.format(e))
#             if self.current_retry < self.max_retries:
#                 self.current_retry += 1
#                 self.log('Retrying in {} seconds...'.format(self.retry_interval))
#                 self.sleep(self.retry_interval)
#             else:
#                 self.log('Maximum retries reached. Exiting...')
#                 self.fail(e)
            

#     def ack(self, tup_id):
#         pass

#     def fail(self, tup_id):
#         self.log('Failed to process tuple: {}'.format(tup_id))
#         # Handle failure of tuple processing here, if needed



# class KafkaSpout(Spout):
#     outputs = ['message']

#     def initialize(self, stormconf, context):
#         self.consumer = KafkaConsumer(
#             'base',
#             bootstrap_servers='localhost:9092',
#             group_id='group1',
#             auto_offset_reset='earliest')
#         self.max_retries = 3  # maximum number of retries
#         self.retry_interval = 5  # retry interval in seconds
#         self.current_retry = 0  # current retry attempt

#     def next_tuple(self):
#         while True:
#             try:
#                 message = self.consumer.poll(timeout_ms=1000, max_records=1)
#                 if not message:
#                     continue
#                 for tp, messages in message.items():
#                     for msg in messages:
#                         self.emit([msg.value.decode('utf-8')])
#                         # self.log(msg)
#                 self.consumer.commit()
#             except errors.KafkaError as e:
#                 self.log('Error: {}'.format(e))
#                 if self.current_retry < self.max_retries:
#                     self.current_retry += 1
#                     self.log('Retrying in {} seconds...'.format(self.retry_interval))
#                     self.sleep(self.retry_interval)
#                 else:
#                     self.log('Maximum retries reached. Exiting...')
#                     self.fail(e)
#                     break

#     def ack(self, tup_id):
#         pass

#     def fail(self, tup_id):
#         self.log('Failed to process tuple: {}'.format(tup_id))
#         # Handle failure of tuple processing here, if needed


from datetime import datetime, timedelta
from collections import deque

class KafkaSpout(Spout):
    outputs = ['message']

    def initialize(self, stormconf, context):
        self.consumer = KafkaConsumer(
            'base',
            bootstrap_servers='localhost:9092',
            group_id='group1',
            auto_offset_reset='earliest')
        self.max_retries = 3  # maximum number of retries
        self.retry_interval = 5  # retry interval in seconds
        self.current_retry = 0  # current retry attempt
        self.tick_interval = 10  # tick interval in seconds
        self.last_tick_time = datetime.now()
        self.tick_queue = deque()

    def next_tuple(self):
        if len(self.tick_queue) > 0:
            tick_tuple = self.tick_queue.popleft()
            self.emit(tick_tuple)
            return

        try:
            message = self.consumer.poll(timeout_ms=1000, max_records=1)
            if not message:
                # Emit a tick tuple if there's no message
                if (datetime.now() - self.last_tick_time).seconds > self.tick_interval:
                    self.tick_queue.append(([None],))
                    self.last_tick_time = datetime.now()
                return

            for tp, messages in message.items():
                for msg in messages:
                    self.emit([msg.value.decode('utf-8')])

                    # self.log(msg)
            self.consumer.commit()

        except errors.KafkaError as e:
            self.log('Error: {}'.format(e))
            if self.current_retry < self.max_retries:
                self.current_retry += 1
                self.log('Retrying in {} seconds...'.format(self.retry_interval))
                self.sleep(self.retry_interval)
            else:
                self.log('Maximum retries reached. Exiting...')
                self.fail(e)
            

    def ack(self, tup_id):
        pass

    def fail(self, tup_id):
        self.log('Failed to process tuple: {}'.format(tup_id))
        # Handle failure of tuple processing here, if needed






