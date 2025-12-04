"""
csv:-
In our ETL testing, we don’t rely on Spark’s inferred schema.
 We first read the file simply, extract schema using df.schema.json(),clean it as per mapping document,
 save it as a schema.json,and then re-read the file using that schema. This ensures correct column names
  and data types exactly as the business mapping
"""

# import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json
from pyspark.sql.types import StructType


# find the schema of cs file
# Create SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

df = spark.read.csv("D:/Etl_Automation_Sep_2025/taf_august/input_files/customer_data/customer_data_01.csv", header=True, inferSchema=True)
print(df.schema.json()) # create schema (Please use JSON format in Google and convert it to JSON as well. )


# find the schema of schema_prep.py (json)
with open("D:/Etl_Automation_Sep_2025/taf_august/tests/table1/schema.json", 'r') as f:
    schema = StructType.fromJson(json.load(f))

print("schema is :-", schema)

# check all detail ok.it shows the schema
df_schema= spark.read.schema(schema).csv("D:/Etl_Automation_Sep_2025/taf_august/input_files/customer_data"
                                         "/customer_data_01.csv", header=True, inferSchema=True)
df_schema.show()
df_schema.printSchema()
