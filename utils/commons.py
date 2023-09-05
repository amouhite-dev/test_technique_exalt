import os
import sys
# add directory source in PATH
fileDirectory = "/".join(os.path.realpath(__file__).split("/")[:-2])
print(f"directory source {fileDirectory}")
sys.path.append(fileDirectory)

import pytz
from glob import glob
from decouple import config
import datetime
from utils.logger import logging

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType
)
USER_DB= config("USER_DB")
PASSWORD_DB= config("PASSWORD_DB")
HOST_DB= config("HOST_DB")
PORT_DB= config("PORT_DB")
DATABASE= config("DATABASE")
COLLECTION = config("COLLECTION")

def logger_message(df, message, type_log="info") -> DataFrame:
    """
    This function logs a message with a specified type and displays the first 5 rows of a DataFrame,
    while also handling any errors that may occur.
    
    :param message: A string message to be logged
    :param df: A DataFrame object that contains data to be logged and displayed
    :param type_log: The type of log message to be logged. It can be "info", "warning", or "error",
    defaults to info (optional)
    :return: a DataFrame.
    """
    if type_log == "error":
        logging.error(message)
    elif type_log == "warning":
        logging.warning(message)
    else:
        logging.info(message)
    try:
        print(df.show(5))
    except Exception as e:
        print('I can not show dataframe. See error : {e}')
        logging.error(f'I can not show dataframe. See error : {e}')
    
    return df 

# Create spark session
def create_session_spark() -> SparkSession:
    spark = SparkSession.builder \
        .appName("FlightRadarETL_Test_Technique") \
        .config("spark.mongodb.input.uri", f"mongodb://{USER_DB}:{PASSWORD_DB}@{HOST_DB}:{PORT_DB}/{DATABASE}?authSource=admin&authMechanism=SCRAM-SHA-256") \
        .config("spark.mongodb.output.uri", f"mongodb://{USER_DB}:{PASSWORD_DB}@{HOST_DB}:{PORT_DB}/{DATABASE}?authSource=admin&authMechanism=SCRAM-SHA-256") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("Success create session spark {spark}")
    logging.info(f"Success create session spark: {spark}")
    return spark

def explode_sub_dicts(data) -> list:
    try:
        data_explode = []
        for d in data:
            exploded = {}
            for key, value in d.items():
                if type(value) == dict:
                    sub_dicts = explode_sub_dicts([value])
                    for sub_dict in sub_dicts:
                        exploded.update({
                            f"{key}_{sub_key}": sub_value
                            for sub_key, sub_value in sub_dict.items()
                        })
                else:
                    exploded[key] = value
            data_explode.append(exploded)
        logging.info("Explode sub dicts is done")
        return data_explode
    except Exception as e:
        print(f"I can not explode sub dicts. See error : {e}")
        logging.error(f"I can not explode sub dicts. See error : {e}")
        return []
    
def get_max_size_dict(spark, data) -> int:    
    """
    This function returns the maximum number of fields in a list of dictionaries. They
    will allow to have all fields in dictionaries.
    
    :param spark: The SparkSession object used to create the DataFrame
    :param data: The input data to create a DataFrame. It is expected to be a list of dictionaries where
    each dictionary represents a row of data. The keys of the dictionaries represent the column names
    and the values represent the values for each column
    :return: index dictionary that have the most field.
    """
    max_field = list({len(d) for d in data})
    df = spark.createDataFrame([(value,) for value in max_field], ["value"])

    return df.agg({"value": "max"}).collect()[0][0] 

def create_dataframe(spark, data) -> DataFrame:
    """
    This function creates a Spark DataFrame from a list of dictionaries with a schema based on the
    maximum number of fields in the dictionaries.
    
    :param spark: The SparkSession object used to create the DataFrame
    :param data: The input data to create a DataFrame. It is expected to be a list of dictionaries where
    each dictionary represents a row of data. The keys of the dictionaries represent the column names
    and the values represent the values for each column
    :return: a DataFrame object.
    """
    max_field = get_max_size_dict(spark, data)
    filtered_data = [d for d in data if len(d) == max_field]
    schema = StructType([StructField(key, StringType(), True) for key in filtered_data[0]])

    rdd = spark.sparkContext.parallelize(filtered_data)
    df = spark.createDataFrame(rdd, schema)

    logging.info("Success create dataframe")
    print(df.show(5))
    return df

def create_parquet_file_if_not_exist(df, category_data, type_data, type_file="parquet"):
    current_date = datetime.datetime.now(pytz.timezone(pytz.country_timezones['FR'][0]))
    name_file = f"Flights/rawzone/{category_data}/{current_date.strftime('%Y')}/{current_date.strftime('%m')}/{current_date.strftime('%d')}/{type_data}_{current_date.strftime('%Y%m%d%H%M%S')}.{type_file}"
    if not os.path.isfile(name_file):
        # df = df.toPandas().DataFrame(columns=df.columns)
        directory = os.path.dirname(name_file)
        os.makedirs(directory, exist_ok=True)
        df.toPandas().to_parquet(name_file, engine='pyarrow', compression='gzip')
        logging.info(f"File {name_file} not existed and is already created")
    return name_file

def save_data_in_file(df: DataFrame, category_data:str, type_data='flights', type_file='parquet') -> None:
    path_file = create_parquet_file_if_not_exist(df, category_data, type_data, type_file)
    df.toPandas().to_parquet(path_file, engine='pyarrow', compression="gzip")
    # df.write.mode('overwrite').parquet(path_file)
    
    print(f"Data of {type_data} in category {category_data} is save with success")
    logging.info(f"Data of {type_data} in category {category_data} is save with success")
    
def convert_datetime_to_str(data):
    if isinstance(data, dict):
        # If the input is a dictionary, iterate over its values recursively
        return {key: convert_datetime_to_str(value) for key, value in data.items()}
    elif isinstance(data, list):
        # If the input is a list, iterate over its elements recursively
        return [convert_datetime_to_str(element) for element in data]
    elif isinstance(data, datetime.date):
        # If the input is a datetime object, convert it to string using strftime
        return data.strftime('%Y-%m-%d %H:%M:%S')
    else:
        # For any other data types, return as is
        return data

def pass_to_dict(df: DataFrame) -> dict:
    # Convert DataFrame rows to a list of dictionaries
    rows = df.collect()
    res = [row.asDict() for row in rows]
    logging.info("Conversion dataframe to dict is done !!!")
    return [convert_datetime_to_str(element) for element in res]
    
def get_last_parquet_file(directory: str) -> str:
    try:
        directory = os.path.dirname(directory)
        parquet_file = glob(os.path.join(directory, '*.parquet'))
        print(f"List files content in directory {parquet_file} \n")
        logging.info(f"List files content in directory {parquet_file} \n")
        return max(parquet_file, key=os.path.getctime) if parquet_file else ""
    except Exception as e:
        print(f"I can not get last parquet file. See error : {e}")
        logging.error(f"I can not get last parquet file. See error : {e}")
        return ""