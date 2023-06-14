import os
import sys

# add directory source in PATH
fileDirectory = "/".join(os.path.realpath(__file__).split("/")[:-2])
print(f"directory source {fileDirectory}")
sys.path.append(fileDirectory)

from utils.logger import logging
from utils.commons import  (
    logger_message, 
    save_data_in_file
)

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    count, 
    when, 
    col, 
    isnan, 
    split, 
    to_timestamp, 
    from_unixtime
)

class TreatmentData:

    def describe_dataframe(self, df:DataFrame) -> None :
        """
        This function takes a DataFrame as input and prints out information about the DataFrame such as
        the first 5 rows, column names, summary statistics, and count of missing values.
        
        :param df: DataFrame - the input dataframe that needs to be described
        :type df: DataFrame
        """
        print("Display my dataframe : ")
        print(df.show(5))
        print()
        
        print(f"List column of dataframe flights is : {df.columns}\n")
        
        print('Describe of my dataframe df_flights : ')
        print(df.describe().show())
        print()
        
        print('Count number of miss value in my dataframe : ')
        print(df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show())
        
        logging.info("Describe dataframe is done !!!")

    def define_type_column_dataframe(self, df: DataFrame) -> DataFrame:
        """
        This function defines the data types of columns in a DataFrame based on their names and returns
        the updated DataFrame.
        
        :param df: DataFrame - the input dataframe that needs to be modified
        :type df: DataFrame
        :return: a DataFrame after defining the data types of its columns and printing the new schema of
        the DataFrame. The function also logs a message indicating that the definition of the new schema
        of the DataFrame is done.
        """
        try:
            for column in df.columns:
                if column.endswith('date'):
                    df = df.withColumn(column, col(column).cast('date'))
                elif column.endswith('scheduled') or column.endswith('estimated') or column.endswith('actual') or column.endswith('runway'):
                    # df = df.withColumn(column, to_timestamp(col(column), "yyyy-MM-dd'T'HH:mm:ssXXX"))
                    df = df.withColumn(column, when(col(column).isNull() | isnan(col(column)), "N/A").otherwise(to_timestamp(col(column), "yyyy-MM-dd'T'HH:mm:ssXXX")))
                else:
                    df = df.withColumn(column, when(col(column).isNull() | isnan(col(column)), "N/A").otherwise(col(column).cast("string")))
            
            print(f"Show new schema of the datframe : \n{df.printSchema()}")
            return logger_message(df, "Definition of new schema of the dataframe is done")
        except Exception as e:
            return logger_message(df, "I can not define schema of dataframe.\nSee dataframe : \n{df}.\nSee error : \n[{e}]", "error")

    def fill_nan_value(self, df:DataFrame) -> DataFrame:
        """
        This function replaces null and NaN values in a DataFrame with the string "N/A".
        
        :param df: a pandas DataFrame containing the data to be processed and cleaned
        :type df: DataFrame
        :return: a DataFrame after replacing all the null and NaN values in each column with the string
        "N/A". The function also logs a message indicating that the replacement is done.
        """
        try:
            for column in df.columns:
                try:
                    df = df.withColumn(column, when(df[column].isNull() | isnan(df[column]), "N/A").otherwise(df[column]))
                except Exception as e:
                    df = df.withColumn(column, "N/A")
                    print(f'I can not replace nan value by string "N/A" in column {column}. See error : {e}')
                    logging.warning(f"I can not replace nan value by string 'N/A' in column {column}. See error : {e}")
            return logger_message(df, "Replace Nan value by string 'N/A' is done", "info")
        except Exception as e:
            print(f'I can not get list columns. See dataframe : \n {df}. Go to see function fill_nan_value. See error : {e}')
            return logger_message(df, f"I can not get list columns. Go to see function fill_nan_value. See error : {e}", "error")

    @staticmethod
    def check_nan_value_column(df, column) -> DataFrame:
        """
        This function replaces null or NaN values in a specified column of a DataFrame with "N/A".
        
        :param df: A pandas DataFrame object that contains the data to be processed
        :param column: The name of the column in the DataFrame that needs to be casted
        :return: a new DataFrame where the specified column has been casted to string type and any null
        or NaN values have been replaced with the string "N/A".
        """
        return when(df[column].isNull() | isnan(df[column]), "N/A").otherwise(df[column])

    def split_timezone(self, df:DataFrame) -> DataFrame:
        """
        This function splits the departure and arrival timezones in a DataFrame into their respective
        continents and cities, and handles any missing values.
        
        :param df: The input DataFrame that contains columns "departure_timezone" and "arrival_timezone"
        :type df: DataFrame
        :return: a DataFrame with new columns "departure_continent", "departure_city",
        "arrival_continent", and "arrival_city" that are derived from splitting the values in the
        "departure_timezone" and "arrival_timezone" columns. The function also logs a message indicating
        that the operation is complete. If an exception occurs, the function logs an error message.
        """
        try:   
            df = df.withColumn("departure_continent", split(df["departure_timezone"], "/").getItem(0)) \
                    .withColumn("departure_city", split(df["departure_timezone"], "/").getItem(1)) \
                    .withColumn("arrival_continent", split(df["arrival_timezone"], "/").getItem(0)) \
                    .withColumn("arrival_city", split(df["arrival_timezone"], "/").getItem(1))

            df = df \
                    .withColumn("departure_continent", self.check_nan_value_column(df, "departure_continent")) \
                    .withColumn("departure_city", self.check_nan_value_column(df, "departure_city")) \
                    .withColumn("arrival_continent", self.check_nan_value_column(df, "arrival_continent")) \
                    .withColumn("arrival_city", self.check_nan_value_column(df, "arrival_city"))

            return logger_message(df, "data columns departure_timezone and arrival_timezone to new column (departure, arrival)_continent and (departure, arrival)_city is done", "info")   
        except Exception as e:
            print(f"I have problem with dataframe {df}. Go to see function split_timezone. See error : {e}")
            return logger_message(df, f"I have problem with dataframe. Go to see function split_timezone. See error : {e}", "error")
        
    def calculate_duration_in_sec_and_hour(self, df: DataFrame) -> DataFrame:
        """
        This function calculates the duration in seconds and hours between two columns in a DataFrame
        and adds two new columns to the DataFrame with the duration in seconds and duration in hour.
        
        :param df: a DataFrame object that contains data to be processed and transformed
        :type df: DataFrame
        :return: a DataFrame with two new columns added: 'duration' in seconds and 'duration_in_hour' in
        hours, minutes, and seconds format. The function also logs a message indicating whether the
        operation was successful or not.
        """
        try:
            df = df.withColumn('duration', when(col('arrival_scheduled').isNull(), None) \
                            .otherwise(col('arrival_scheduled').cast('long') - col('departure_scheduled').cast('long')))
            
            df = df.withColumn("duration_in_hour", from_unixtime(col("duration"), "HH:mm:ss"))

            return logger_message(df, "Creation columns duration in seconds and duration in hour is done", "info") 
        except Exception as e:
            print(f"I have problem with dataframe {df}. Go to see function calculate_duration_in_sec_and_hour. See error : {e}")
            return logger_message(df, f"I have problem with dataframe. Go to see function calculate_duration_in_sec_and_hour. See error : {e}", "error")

    def runner(self, df) -> DataFrame:
        """
        The function takes a dataframe as input, performs various data cleaning and transformation
        operations with pyspark, and returns the resulting dataframe.
        
        :param df: The input DataFrame that will be processed by the `runner` method
        :return: The function `runner` returns a Pandas DataFrame object.
        """

        # data_extracted = get_last_parquet_file(self.directory)
        
        # df = spark.read.parquet(data_extracted)
        
        self.describe_dataframe(df)
        
        df = self.define_type_column_dataframe(df)

        df = self.split_timezone(df)
        
        df = self.fill_nan_value(df)

        df = self.calculate_duration_in_sec_and_hour(df)

        self.describe_dataframe(df)
        
        save_data_in_file(df, "transformation")
        
        return df
