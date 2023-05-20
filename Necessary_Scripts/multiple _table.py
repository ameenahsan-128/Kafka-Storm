from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, input_file_name
from pyspark.sql import Row
import json
import os

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Load JSON into Hive Table") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Specify the table name
table_name = "per"
table_name1= "loc"

# Create the table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Name STRING,
        Age STRING,
        DOB STRING,
        Email STRING,
        Phone STRING,
        input_file_name STRING
    )
""")
          

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name1} (
        address STRING,
        postalcode STRING,
        city STRING,
        CountryCode STRING,
        input_file_name STRING
    )
""")

# Read the existing data from the table to check for duplicates
existing_data_df = spark.table(table_name)

base_directory = "/home/ameen/datas/"

# Define a function to process the streaming data
def process_streaming_data(df, epoch_id):
    # Get the input file name for each record
   

#Get a list of JSON files in the json_file_location directory and its subdirectories
    json_files = [os.path.join(dirpath, filename) for dirpath, dirnames, filenames in os.walk(base_directory) for filename in filenames if filename.endswith(".json")]

    # Filter out the JSON files that are already in the table
    new_json_files = [file for file in json_files if file not in existing_data_df.select("input_file_name").distinct().rdd.flatMap(lambda x: x).collect()]

    # Iterate over the new JSON files and append them to the table
    for json_file in new_json_files:
        # Read the JSON file as text
        with open(json_file, 'r') as file:
            json_text = file.read()

        try:
        # Parse the JSON string
            json_obj = json.loads(json_text)
            json_person=json_obj.get('Personal_info')
            print(json_person)
            # Create a Row object with the parsed JSON data
            new_data_row = Row(
                Name=json_person.get("name"),
                Age=json_person.get("Age"),
                DOB=json_person.get("DOB"),
                Email=json_person.get("email"),
                Phone=json_person.get("phone"),
                input_file_name=json_file
                )

                # Create a DataFrame from the Row object
            new_data_personal = spark.createDataFrame([new_data_row])

                # Save the new DataFrame to the table
            new_data_personal.write.mode("append").format("hive").saveAsTable(table_name)

            json_obj = json.loads(json_text)
            json_location=json_obj.get('location')
            print(json_location)
            # Create a Row object with the parsed JSON data
            row_location = Row(
                Address=json_location.get("address"),
                Postalcode=json_location.get("postalCode"),
                City=json_location.get("city"),
                Countrycode=json_location.get("countryCode"),
                input_file_name=json_file
                )

                # Create a DataFrame from the Row object
            new_data_loc = spark.createDataFrame([row_location])

                # Save the new DataFrame to the table
            new_data_loc.write.mode("append").format("hive").saveAsTable(table_name1)

        except ValueError:
            # Invalid JSON data, skip it
            continue


# Create a streaming DataFrame by monitoring the JSON file location
streaming_personal= spark.readStream.format("json").schema("Name STRING, Age STRING, DOB STRING, Email STRING, Phone STRING, input_file_name STRING").load(base_directory + "/**/*.json")
streaming_location = spark.readStream.format("json").schema("Name STRING, Age STRING, DOB STRING, Email STRING, Phone STRING, input_file_name STRING").load(base_directory + "/**/*.json")

# Apply the processing function to the streaming DataFrame
streaming_query_per = streaming_personal.writeStream.foreachBatch(process_streaming_data).start()
streaming_query_loc = streaming_location.writeStream.foreachBatch(process_streaming_data).start()


# Wait for the streaming query to terminate
streaming_query_per.awaitTermination()
streaming_query_loc.awaitTermination()

