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
table_name = "usernew"

# Create the table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Name STRING,
        Age STRING,
        DOB STRING,
        Email STRING,
        Phone STRING,
        signup_at STRING,
        input_file_name STRING
    )
""")

# Read the existing data from the table to check for duplicates
existing_data_df = spark.table(table_name)

# Specify the base directory for JSON files
base_directory = "hdfs://localhost:9000/json_user"

# Define a function to process the streaming data
def process_streaming_data(df, epoch_id):
    # Get the input file name for each record
    df_with_filename = df.withColumn("input_file_name", input_file_name())

    # Filter out the records that are already in the table
    new_data_df = df_with_filename.filter(~df_with_filename.input_file_name.isin(existing_data_df.select("input_file_name").distinct().rdd.flatMap(lambda x: x).collect()))

    # Iterate over the new records and append them to the table
    for row in new_data_df.collect():
        try:
            # Create a Row object with the parsed JSON data
            new_data_row = Row(
                Name=row["Name"],
                Age=row["Age"],
                DOB=row["DOB"],
                Email=row["Email"],
                Phone=row["Phone"],
                signup_at=row["signup_at"],
                input_file_name=row["input_file_name"]
            )

            # Create a DataFrame from the Row object
            new_data_df = spark.createDataFrame([new_data_row])

            # Save the new DataFrame to the table
            new_data_df.write.mode("append").format("hive").saveAsTable(table_name)

        except ValueError:
            # Invalid JSON data, skip it
            continue

# Define a function to process the existing JSON files
def process_existing_files():
    # Read all JSON files from the base directory and its subdirectories
    json_files_df = spark.read.json(base_directory + "/**/*.json", multiLine=True)

    # Apply the processing function to the DataFrame
    process_streaming_data(json_files_df, None)

# Check if the JSON files have already been processed
existing_files_processed = spark.catalog.tableExists(table_name)

# Process the existing JSON files only if they haven't been processed before
if not existing_files_processed:
    process_existing_files()

# Create a streaming DataFrame by monitoring the JSON file location
streaming_df = spark.readStream.format("json").schema("Name STRING, Age STRING, DOB STRING, Email STRING, Phone STRING, signup_at STRING").load(base_directory + "/**/*.json")

# Apply the processing function to the streaming DataFrame
streaming_query = streaming_df.writeStream.foreachBatch(process_streaming_data).start()

# Wait for the streaming query to terminate
streaming_query.awaitTermination()
