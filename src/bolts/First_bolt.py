from hdfs import InsecureClient
import json
from queue import Queue
import logging
from streamparse import Bolt



# class Emit(Bolt):
#     outputs = ['filtered_user']

#     def process(self, tup):
#         json_data = tup.values[0]  # Assuming the JSON data is stored in the first field of the tuple
#         try:
#             user_data = json.loads(json_data)
#             user_name = user_data.get('personal_info')
#             folder_name=user_name['Name']
#             folder_name = folder_name.replace(" ", "")
#             if user_name is not None:
#                 self.log(user_name)
#                 self.log(folder_name)
#         except ValueError:
#             pass
#         except TypeError:
#             pass

class Emit(Bolt):


    def initialize(self, storm_conf, context):
        self.hdfs_client = InsecureClient('http://localhost:50070',user='hdfs')  # replace with appropriate HDFS address

    def process(self, tup):
        json_data = tup.values[0]  # Assuming the JSON data is stored in the first field of the tuple
        try:
            user_data = json.loads(json_data)
            user_name = user_data.get('personal_info')
            folder_name=user_name['Name']
            folder_name = folder_name.replace(" ", "")
            if user_name is not None:
                # self.log(user_name)
                # self.log(folder_name)
                with self.hdfs_client.write('/chakka/' + folder_name + '/user_name.txt', encoding='utf-8') as writer:
                    writer.write(str(user_name))
                    self.log(f'{folder_name} added to hdfs')
        except ValueError:
            pass
        except TypeError:
            pass




# from hdfs.client import InsecureClient

# class Emit(Bolt):
#     outputs = ['filtered_user']

#     def __init__(self):
#         self._hdfs_client = InsecureClient('http://localhost:50070', user='hdfs')

#     def process(self, tup):
#         # Get the JSON data from the input tuple
#         json_data = tup.values[0]

#         try:
#             # Convert the JSON data to a dictionary
#             if isinstance(json_data, bytes):
#                 json_data = json_data.decode('utf-8')
#             user_data = json.loads(json_data)

#             # Get the user's name from the personal_info field
#             personal_info = user_data.get('personal_info')
#             if personal_info is not None:
#                 user_name = personal_info['Name'].replace(" ", "")
#                 # self.emit([personal_info])
#                 self.log(personal_info)

#                 # Set the output file path based on the user's name
#                 output_file = f'/hanna/{user_name}/{user_name}.txt'
#                 self.log(output_file)

#                 # Write the user's personal info to the output file on HDFS
#                 with self._hdfs_client.write(output_file, overwrite=False) as writer:
#                     # Encode the personal info dictionary as JSON and write it to the file
#                     personal_info_json = json.dumps(personal_info)
#                     writer.write(personal_info_json.encode("utf8"))

#         except (ValueError, TypeError) as e:
#             self.log(f'Error processing tuple: {e}')
#             pass


# class Emit(Bolt):
#     outputs = ['filtered_user']

#     def __init__(self):
#         self._hdfs_client = InsecureClient('http://localhost:50070', user='hdfs')

#     def process(self, tup):
#         # Get the JSON data from the input tuple
#         json_data = tup.values[0]

#         try:
#             # Convert the JSON data to a dictionary
#             if isinstance(json_data, bytes):
#                 json_data = json_data.decode('utf-8')
#             user_data = json.loads(json_data)

#             # Filter the user data to only include certain fields
#             filtered_user_data = {
#                 'personal_info': {
#                     'Name': user_data['personal_info']['Name'],
#                     'Age': user_data['personal_info']['Age'],
#                     'Gender': user_data['personal_info']['Gender']
#                 },
#                 'address': user_data['address']
#             }

#             # Get the user's name from the personal_info field
#             personal_info = user_data.get('personal_info')
#             if personal_info is not None:
#                 user_name = personal_info['Name'].replace(" ", "")

#                 # Set the output file path based on the user's name
#                 output_file = f'/hanna/{user_name}/{user_name}.txt'

#                 # Write the filtered user data to the output file on HDFS
#                 with self._hdfs_client.write(output_file, overwrite=False) as writer:
#                     # Encode the filtered user data dictionary as JSON and write it to the file
#                     filtered_user_data_json = json.dumps(filtered_user_data)
#                     writer.write(filtered_user_data_json.encode("utf8"))

#                 # Emit the filtered user data to the next bolt
#                 self.emit([filtered_user_data])

#         except (ValueError, TypeError) as e:
#             self.log(f'Error processing tuple: {e}')
#             pass




# from queue import Queue

# class YourClass:
#     def __init__(self):
#         self._queue = Queue()

#     def your_method(self):
#         for key, values in schema.items():
#             for person in values:
#                 name = person['Personal_info']
#                 folder_name = name['name']
#                 folder_name = folder_name.replace(" ", "")
#                 print(folder_name)
#                 output_file = f'/user/2023/hanna/{folder_name}/{folder_name}.txt'
#                 print(output_file)
                
#                 # Open the output file for writing
#                 with client.write(output_file, overwrite=False) as writer:
#                     # Write the personal info for this person to the output file
#                     personal_info_json = str(person['Personal_info'])
#                     print(personal_info_json)
#                     # Encode the JSON string to bytes and write to the file
#                     writer.write(personal_info_json.encode("utf8"))


# # Create an instance of YourClass
# your_object = YourClass()

# # Call the your_method function
# your_object.your_method()