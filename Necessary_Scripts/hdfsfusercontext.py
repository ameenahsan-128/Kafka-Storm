from hdfs import InsecureClient
import json

# Connect to HDFS
client = InsecureClient('http://localhost:50070', user='hdfs')

schema = {"person":[
  {"Personal_info": {
        "name": "maria",
          "Age":"34",
          "DOB": "19-11-1989",
        "email": "john@gmail.com",
        "phone": "(91)7869569810"
      },
   "location": {
        "address": "21734 broadway st",
        "postalCode": "CA 94115",
            "city": "Delhi",
      "countryCode": "IN"
    },
    "education":{
          "area": "Software Development",
      "studyType": "Bachelor",
      "institution": "Kerala University",
      "startDate": "04-06-2009",
      "endDate": "03-05-2013",
      "score": "4.0"
       },
    "skills":{
          "Softskills":["Englishspeaking","Englishwriting","Leadership"],
          "Techskills":["python","Hadoop","HTML","Nodejs","Angular"]
    }
}, {"Personal_info": {
        "name": "Thasni",
          "Age":"35",
          "DOB": "19-11-1999",
        "email": "maria@gmail.com",
        "phone": "(91)7869569810"
      },
   "location": {
        "address": "location in kochi",
        "postalCode": "CA 94115",
            "city": "kochi",
      "countryCode": "IN"
    },
   "education":{
          "area": "Software Development",
      "studyType": "Post Graduate",
      "institution": "Kerala University",
      "startDate": "04-06-2009",
      "endDate": "03-05-2013",
      "score": "5.0"
       },
    "skills":{
          "Softskills":["Englishspeaking","Englishwriting","Leadership"],
          "Techskills":["Java","CSS","Javascript","Hadoop","HTML","Nodejs","Angular"]
    }
}]}

# # Define the local file path and the HDFS destination path
# local_file_path = '/root/project/hbaseeee.py'
# hdfs_dest_path = '/data/file.txt'

# # Upload the local file to HDFS
# client.upload(hdfs_dest_path, local_file_path)

# # Verify if the file exists in HDFS
# if client.status(hdfs_dest_path):
#     print("File uploaded successfully to HDFS.")
# else:
#     print("Error uploading file to HDFS.")

# for key, values in schema.items():
#     for person in values:
#         name = person['Personal_info']
#         folder_name=name['name']
#         folder_name = folder_name.replace(" ", "")
#         print(folder_name)
#         output_file = f'/user/2023/nisam/{folder_name}/{folder_name}.txt'
#         print(output_file)
            
#         # Open the output file for writing
#         with client.write(output_file,overwrite=False) as writer:
#             # Write the personal info for this person to the output file
#             personal_info_json = str(person['Personal_info'])
#             print(personal_info_json)
#             # Encode the JSON string to bytes and write to the file
#     writer.write(personal_info_json.encode("utf8"))


# from queue import Queue

# class YourClass:
#     def __init__(self):
#         self._queue = Queue()

#     def your_method(self):
#         for key, values in schema.items():
#             for person in values:
#                 name = person['Personal_info']
#                 folder_name=name['name']
#                 folder_name = folder_name.replace(" ", "")
#                 print(folder_name)
#                 output_file = f'/user/2023/ameen1/{folder_name}/{folder_name}.txt'
#                 print(output_file)
            
#         # Open the output file for writing
#         with client.write(output_file,overwrite=False) as writer:
#             # Write the personal info for this person to the output file
#             personal_info_json = str(person['Personal_info'])
#             print(personal_info_json)
#             # Encode the JSON string to bytes and write to the file
#         writer.write(str(personal_info_json.encode("utf8")))
#                     # Dequeue the chunk from the queue
#         chunk = self._queue.get()
#         # Write the personal info for this person to the output file
#         writer.write(chunk)

# # Create an instance of YourClass
# your_object = YourClass()

# # Call the your_method function
# your_object.your_method()



from queue import Queue

class YourClass:
    def __init__(self):
        self._queue = Queue()

    def your_method(self):
        for key, values in schema.items():
            for person in values:
                name = person['Personal_info']
                folder_name = name['name']
                folder_name = folder_name.replace(" ", "")
                print(folder_name)
                output_file = f'/kakakak/{folder_name}/{folder_name}.txt'
                print(output_file)
                
                # Open the output file for writing
                with client.write(output_file, overwrite=False) as writer:
                    # Write the personal info for this person to the output file
                    personal_info_json = str(person['Personal_info'])
                    print(personal_info_json)
                    # Encode the JSON string to bytes and write to the file
                    writer.write(personal_info_json.encode("utf8"))


# Create an instance of YourClass
your_object = YourClass()

# Call the your_method function
your_object.your_method()