import os
import sys
# add directory source in PATH
fileDirectory = "/".join(os.path.realpath(__file__).split("/")[:-2])
print(f"directory source {fileDirectory}")
sys.path.append(fileDirectory)

import requests
from typing import *

from pyspark.sql import DataFrame

from utils.logger import logging
from utils.commons import  (
    create_dataframe, 
    explode_sub_dicts, 
    save_data_in_file
)

class ExtractionData:
    url = ""
    params = {}
    type_data = ""
    def __init__(self, url, params, type_data):
        self.url = url
        self.params = params
        self.type_data = type_data
        
    def get_data(self) -> list:    
        """
        This function sends a GET request to a specified URL with parameters and returns the JSON data
        from the response, self.logger any errors that occur.
        :return: A list of data extracted from the API response, or an empty list if there was an error
        while getting the data.
        """
        try:
            response = requests.get(self.url, params=self.params)
            logging.info("Extraction data is done")
            return response.json()['data']
        except Exception as e:
            logging.error(f'Extraction data failed. See error :  [{e}]')
            return []

    def describe_data_extract(self, data:list, type_data: str) -> None:
        """
        This function takes in a list of data and a string representing the type of data, and outputs
        information about the data including its size and first element.
        
        :param data: a list of data that needs to be described
        :type data: list
        :param type_data: The type of data that is being describe. In this case, it flights data.
        :type type_data: str
        """
        if data:
            print(f"""
                [\n
                    'Type data': {type_data};\n
                    'Size':{len(data)};\n
                    'First_element': {data[0]}
                \n]
            """)
        else:
            print(f"""
                [\n
                    'Type data': {type_data};\n
                    'Size':{len(data)};\n
                    'First_element': {data}
                \n]
            """)
        logging.info("Describe data extract is done")
    
    def runner(self, spark) -> DataFrame:
        """
        This function extracts flight data, explodes sub-dicts in a list dict, creates a dataframe, saves
        the data in parquet format, and returns the dataframe.
        
        :param spark: The spark parameter is an instance of the SparkSession class, which is the entry point
        to programming Spark with the Dataset and DataFrame API. It is used to create DataFrame objects and
        perform operations on them
        :return: The function `runner` returns a DataFrame object.
        """
        flights = self.get_data()
        
        self.describe_data_extract(flights, self.type_data)
        
        flights = explode_sub_dicts(flights)
        
        df = create_dataframe(spark, flights)
        
        save_data_in_file(df, "extraction")
        
        return df