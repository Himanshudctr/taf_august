from pyspark.sql.functions import col, when
from src.utility.report_lib import write_output


def schema_check(source, target, spark):
    # Extract schema details as lists of (column_name, data_type) tuples
    source_schema = source.schema
    target_schema = target.schema

    # Convert schemas to DataFrames
    source_schema_df = spark.createDataFrame(
        [(field.name.lower(), field.dataType.simpleString()) for field in source_schema],
        ["col_name", "source_data_type"]
    )

    target_schema_df = spark.createDataFrame(
        [(field.name.lower(), field.dataType.simpleString()) for field in target_schema],
        ["col_name", "target_data_type"]
    )

    # Perform a full join on column names and compare data types
    schema_comparison = (
        source_schema_df.alias("src")
        .join(target_schema_df.alias("tgt"), col("src.col_name") == col("tgt.col_name"), "full_outer")
        .select(
            col("src.col_name").alias("source_col_name"),
            col("tgt.col_name").alias("target_col_name"),
            col("src.source_data_type"),
            col("tgt.target_data_type"),
            when(col("src.source_data_type") == col("tgt.target_data_type"), "pass")
            .otherwise("fail")
            .alias("status")
        )
    )

    # Filter only rows where the status is 'fail'
    failed = schema_comparison.filter(col("status") == "fail")
    failed.show()
    failed_count = failed.count()

    if failed_count > 0:
        failed_records = failed.collect()
        failed_preview = [row.asDict() for row in failed_records]  # Convert rows to a dictionary for display
        status = "FAIL"
        write_output(
            "Schema Check",
            status,
            f"schema failed columns Count: {failed_count}, Sample Failed Records: {failed_preview}"
        )
        return status
    else:
        status = "PASS"
        write_output("scheck Check", status, "Schema is correct!")
        return status

#----------Information--------best information -----------------------------------------------------------
# """
# how to create schema check / how to inputs the function takes.
#
#   # Convert schemas to DataFrames
#     source_schema_df = spark.createDataFrame(
#         [(field.name.lower(), field.dataType.simpleString()) for field in source_schema],
#         ["col_name", "source_data_type"]
#     )
# """
# # Import SparkSession
# from pyspark.sql import SparkSession
# import json
# from pyspark.sql.types import StructType
#
# # Create SparkSession
# spark = SparkSession.builder \
#       .master("local[1]") \
#       .appName("SparkByExamples.com") \
#       .getOrCreate()
#
#
# df = spark.read.csv("D:/Etl_Automation_Sep_2025/taf_august/input_files/customer_data/customer_data_01.csv", header=True, inferSchema=True)
# df.show()
#
# source_schema = df.schema
#
# list1 = []
# for field in source_schema:
#     # print(field)
#     #
#     # print("field name", field.name)
#     # print("field datatype", field.dataType.simpleString())
#
#     list1.append((field.name.lower(), field.dataType.simpleString()))
# print(list1)
# source_schema_df = spark.createDataFrame(list1,["col_name", "source_data_type"])
#
# source_schema_df.show()
