
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import Row
import json
import os

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Load JSON into Hive Table") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("mapreduce.input.fileinputformat.input.dir.recursive", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# Specify the table name
table_name = "locatiadannson"

# Create the table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        address STRING,
        postalcode STRING,
        city STRING,
        CountryCode STRING,
        input_file_name STRING
    )
""")

# Read the existing data from the table to check for duplicates
existing_data_df = spark.table(table_name)

# Specify the JSON file location
json_file_location = "/home/ameen/datas/"

#Get a list of JSON files in the json_file_location directory and its subdirectories
json_files = [os.path.join(dirpath, filename) for dirpath, dirnames, filenames in os.walk(json_file_location) for filename in filenames if filename.endswith(".json")]

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
        json_location=json_obj.get('location')
        print(json_location)
        
        # Create a Row object with the parsed JSON data
        row = Row(
            Address=json_location.get("address"),
            Postalcode=json_location.get("postalCode"),
            City=json_location.get("city"),
            Countrycode=json_location.get("countryCode"),
            input_file_name=json_file
        )
        
        # Create a DataFrame from the Row object
        new_data_df = spark.createDataFrame([row])
        
        # Save the new DataFrame to the table
        # new_data_df.write.mode("append").saveAsTable(table_name)
        new_data_df.write.mode("append").format("hive").saveAsTable(table_name)
        
    except ValueError:
        # Invalid JSON file, skip it
        continue

# Execute the query and retrieve the result as a DataFrame
df = spark.sql(f"SELECT * FROM {table_name}")

# Show the content of the DataFrame
df.show(100)

# Stop the SparkSession
spark.stop()

