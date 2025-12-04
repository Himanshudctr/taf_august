import pytest
from pyspark.sql import SparkSession
import os
import yaml
from src.utility.read_file_lib import read_file
from src.utility.read_db_lib import read_db
import logging     # logging pass form the pytest.ini file  (day_70(2)_00:47)


@pytest.fixture(scope='session')
def spark_session():
    print("this is spark session fixture")
    taf_august = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # before taf_august file
    sql_path = os.path.join(taf_august, 'jars', 'mssql-jdbc-12.2.0.jre8.jar')
    oracle_path = os.path.join(taf_august, 'jars', 'ojdbc8.jar')
    mysql_path = os.path.join(taf_august, 'jars', 'mysql-connector-java-8.0.29.jar')
    postgres_path = os.path.join(taf_august, 'jars', 'postgresql-42.6.2.jar')
    # jar_path = r"D:\Etl_Automation_Sep_2025\taf_august\jars\mssql-jdbc-12.2.0.jre8.jar"
    jar_path = sql_path + ',' + oracle_path + ',' + mysql_path + ',' + postgres_path
    print("jar_path", jar_path)
    spark = (SparkSession.builder.master('local[1]')
             .config("spark.jars", jar_path)
             .config("spark.driver.extraClassPath", jar_path)
             .config("spark.executor.extraClassPath", jar_path)
             .appName("ETL Automation FW").getOrCreate())

    # spark.sparkContext.setLogLevel("INFO")
    return spark


@pytest.fixture(scope='module')
def read_config(request):
    dir_path = request.node.fspath.dirname  #
    config_path = os.path.join(dir_path, 'config.yml')
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)

    return config_data


@pytest.fixture(scope='module')
def read_data(spark_session, read_config, request):
    spark = spark_session
    config_data = read_config

    # this is read schema.json ( request fixture for schem_read )
    dir_path = request.node.fspath.dirname
    source_config = config_data['source']
    target_config = config_data['target']
    validation_config = config_data['validations']  # validation of config.yml ( read_config..'Tahble1')

    source_config = config_data['source']
    print("==" * 50)
    print("source config", source_config)
    target_config = config_data['target']
    print("==" * 50)
    print("target config", target_config)
    #
    # print("source_config['type']", source_config['type'])
    # print("source_config['path']", source_config['path'])
    # print("source_config['options']", source_config['options'])

    if source_config['type'] == 'database':
        source_df = read_db(config=source_config, spark=spark, dir_path=dir_path)
    else:
        # source_df = spark.read.csv(path=source_config['path'], header=source_config['options']['header'])
        # source_df.show()
        source_df = read_file(config=source_config, spark=spark, dir_path=dir_path)

    if target_config['type'] == 'database':
        target_df = read_db(config=target_config, spark=spark, dir_path=dir_path)
    else:
        target_df = read_file(config=target_config, spark=spark, dir_path=dir_path)

    return source_df.drop(*source_config['exclude_cols']), target_df.drop(
        *target_config['exclude_cols']), validation_config
