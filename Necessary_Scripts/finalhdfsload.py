from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, input_file_name
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import json
import os
import glob
import subprocess

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Load JSON into Hive Table") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Specify the table names
table_name = "personal"
table_name1 = "location"
table_name2 = "education"
table_name3 = "skills"
table_name4 = "works"
table_name6 = "certificates"
table_name7 = "projects"

# Create the tables if they don't exist
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

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name2} (
        area STRING,
        studyType STRING,
        institution STRING,
        startDate STRING,
        endDate STRING,
        score STRING,
        input_file_name STRING
    )
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name3} (
        Softskills ARRAY<STRUCT<
            Englishspeaking STRING,
            Englishwriting STRING,
            Leadership STRING
        >>,
        Techskills ARRAY<STRUCT<
            Skill STRING,
            Rating STRING
        >>,
        input_file_name STRING
    )
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name4} (
        Company_Name STRING,
        Possition STRING,
        Start_date STRING,
        EndDate STRING,
        input_file_name STRING
    )
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name6} (
        Certificates ARRAY<STRUCT<
            name STRING,
            date STRING,
            issuer STRING
        >>,
        input_file_name STRING
    )
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name7} (
        Projects ARRAY<STRUCT<
            Project_name STRING,
            discription STRING,
            role STRING
        >>,
        input_file_name STRING
    )
""")

# Read the existing data from each table to check for duplicates
existing_data_df = {
    table_name: spark.table(table_name).select("input_file_name").distinct(),
    table_name1: spark.table(table_name1).select("input_file_name").distinct(),
    table_name2: spark.table(table_name2).select("input_file_name").distinct(),
    table_name3: spark.table(table_name3).select("input_file_name").distinct(),
    table_name4: spark.table(table_name4).select("input_file_name").distinct(),
    table_name6: spark.table(table_name6).select("input_file_name").distinct(),
    table_name7: spark.table(table_name7).select("input_file_name").distinct(),
}

base_directory = "hdfs://localhost:9000/schema"

def process_streaming_data(df, epochid):
    def list_json_files(directory):
        files = []
        process = subprocess.Popen(["hadoop", "fs", "-ls", "-R", directory], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = process.communicate()
        if process.returncode == 0:
            lines = output.decode().split("\n")
            for line in lines[1:]:
                parts = line.split()
                if len(parts) >= 8 and parts[0][0] != "d" and parts[7].endswith(".json"):
                    files.append(parts[7])
        return files


    # Get a list of JSON files in the base_directory and its subdirectories
    json_files = list_json_files(base_directory)

    # Iterate over the new JSON files and append them to the corresponding tables
    for json_file in json_files:
        # Check if the JSON file is already processed for each table
        file_exists = {
            table: json_file in existing_data_df[table].rdd.flatMap(lambda x: x).collect()
            for table in existing_data_df
        }

        # Read the JSON file as text
        json_text = spark.read.text(json_file).first()[0]

        try:
            # Parse the JSON string
            json_obj = json.loads(json_text)

            # Process the JSON data for each table if the file doesn't exist in that table
            if not file_exists[table_name]:
                json_person = json_obj.get("Personal_info")
                new_data_row = Row(
                    Name=json_person.get("name"),
                    Age=json_person.get("Age"),
                    DOB=json_person.get("DOB"),
                    Email=json_person.get("email"),
                    Phone=json_person.get("phone"),
                    input_file_name=json_file,
                )
                new_data_personal = spark.createDataFrame([new_data_row])
                new_data_personal.write.mode("append").format("hive").saveAsTable(table_name)

            if not file_exists[table_name1]:
                json_location = json_obj.get("location")
                row_location = Row(
                    address=json_location.get("address"),
                    postalcode=json_location.get("postalCode"),
                    city=json_location.get("city"),
                    CountryCode=json_location.get("countryCode"),
                    input_file_name=json_file,
                )
                new_data_loc = spark.createDataFrame([row_location])
                new_data_loc.write.mode("append").format("hive").saveAsTable(table_name1)

            if not file_exists[table_name2]:
                json_education = json_obj.get("education")
                row_education = Row(
                    area=json_education.get("area"),
                    studyType=json_education.get("studyType"),
                    institution=json_education.get("institution"),
                    startDate=json_education.get("startDate"),
                    endDate=json_education.get("endDate"),
                    score=json_education.get("score"),
                    input_file_name=json_file,
                )
                new_data_edu = spark.createDataFrame([row_education])
                new_data_edu.write.mode("append").format("hive").saveAsTable(table_name2)

            if not file_exists[table_name3]:
                skills = json_obj.get("skills")
                softskills = skills.get("Softskills")
                softskills_struct = [
                    Row(
                        Englishspeaking=item.get("Englishspeaking"),
                        Englishwriting=item.get("Englishwriting"),
                        Leadership=item.get("Leadership")
                    )
                    for item in softskills
                ]

                # Techskills
                techskills = skills.get("Techskills")
                techskills_struct = [
                    Row(
                        Skill=list(item.keys())[0],
                        Rating=list(item.values())[0]
                    )
                    for item in techskills
                ]

                # Create a Row object with the parsed JSON data
                new_data_row_struct = Row(
                    Softskills=softskills_struct,
                    Techskills=techskills_struct,
                    input_file_name=json_file
                )

                # Create a DataFrame from the Row object
                new_data_df = spark.createDataFrame([new_data_row_struct])

                # Save the new DataFrame to the table
                new_data_df.write.mode("append").format("hive").saveAsTable(table_name3)

            if not file_exists[table_name4]:
                json_work = json_obj.get("work")
                new_data_row_work = Row(
                   Company_Name=json_work.get("name"),
                    Possition=json_work.get("position"),
                    Start_date=json_work.get("startDate"),
                    EndDate=json_work.get("endDate"),
                    input_file_name=json_file,
                )
                new_data_personal = spark.createDataFrame([new_data_row_work])
                new_data_personal.write.mode("append").format("hive").saveAsTable(table_name4)


            if not file_exists[table_name6]:
                # Certificates
                certificates = json_obj.get("certificates")
                certificates_struct = [
                    Row(
                        name=certificate.get("name"),
                        date=certificate.get("date"),
                        issuer=certificate.get("issuer")
                    )
                    for certificate in certificates
                ]

                # Create a Row object with the parsed JSON data
                new_data_row_struct = Row(
                    Certificates=certificates_struct,
                    input_file_name=json_file,
                )

                # Create a DataFrame from the Row object
                new_data_df = spark.createDataFrame([new_data_row_struct])

                # Save the new DataFrame to the table
                new_data_df.write.mode("append").format("hive").saveAsTable(table_name6)


            if not file_exists[table_name7]:
                # Certificates
                projects= json_obj.get("projects")
                project_struct = [
                    Row(
                        Project_name=project.get("name"),
                        discription=project.get("description"),
                        role=project.get("role")
                    )
                    for project in projects
                ]

                # Create a Row object with the parsed JSON data
                new_data_row_struct = Row(
                    projects=project_struct,
                    input_file_name=json_file,
                )

                # Create a DataFrame from the Row object
                new_data_df = spark.createDataFrame([new_data_row_struct])

                # Save the new DataFrame to the table
                new_data_df.write.mode("append").format("hive").saveAsTable(table_name7)

        except ValueError:
            # Invalid JSON data, skip it
            continue


# Create a streaming DataFrame by monitoring the JSON file location
streaming_personal = spark.readStream.format("json").schema(
    "Name STRING, Age STRING, DOB STRING, Email STRING, Phone STRING, input_file_name STRING"
).load(base_directory + "/**/*.json")

# Apply the processing function to the streaming DataFrame
streaming_query_per = streaming_personal.writeStream.foreachBatch(process_streaming_data).start()

# Wait for the streaming query to terminate
streaming_query_per.awaitTermination()
