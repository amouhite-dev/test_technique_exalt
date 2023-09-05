import os
import sys
# add directory source in PATH
fileDirectory = "/".join(os.path.realpath(__file__).split("/")[:-2])
print(f"directory source {fileDirectory}")
sys.path.append(fileDirectory)

from utils.logger import logging
from utils.commons import logger_message

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StructType
from pyspark.sql.functions import (
    avg,
    abs,
    col, 
    desc, 
    when,
    count,
    isnan, 
    array_min,
    dense_rank,
    row_number,
    collect_list,
    from_unixtime,
)


class RequestData:
    # directory = ""
    spark = None
    def __init__(self, spark):
        self.spark = spark
        # self.directory = f"Flights/rawzone/transformation/tech_year={datetime.now().strftime('%Y')}/tech_month={datetime.now().strftime('%Y-%m')}/tech_day={datetime.now().strftime('%Y-%m-%d')}"
    
    def get_airline_with_more_flights(self, df: DataFrame) -> None:
        """
        This function takes a DataFrame of flight data, determines if there are any active flights, and
        returns the airline with the most flights (either active or scheduled) along with a log message.
        
        :param df: The input parameter is a DataFrame containing flight data
        :type df: DataFrame
        :return: a logger message with the result of the query to get the airline with more flights,
        either active or scheduled, from a given DataFrame. The message includes the airline name and
        the number of flights, and it is logged as an info level message.
        """
        try:
            status_flight = "scheduled"
            flight_status = df.filter(col('flight_status')=="active") # get by filter flights actif
            number_flights_by_airline = df.groupBy('airline_name').count().orderBy(desc("count")) # get number flights by airline_name
            
            # check if flights active exist, else I use flights scheduled(many flights scheduled available in dataset)
            if flight_status.count() > 0:
                number_flights_by_airline = flight_status.groupBy('airline_name').count().orderBy(desc("count"))
                status_flight = "active"
            else:
                print(f"I can not get airline with the most flights actif. See result : \n{flight_status.show()}")
                logging.warning("I can not get airline actif with the most flights actif")

            print(f"Airline with the most flights {status_flight} is {number_flights_by_airline.first()}.")
            logging.info(f"Airline with the most flights {status_flight} is {number_flights_by_airline.first()}.")
            
            return logger_message(
                number_flights_by_airline,
                "[REQUEST] -> Get airline with the most flights is done."
            )
        except Exception as e:
            return logger_message(
                self.spark.createDataFrame([], StructType([])),
                f"Error to get airline with the most flights. See function get_airline_with_more_flights. See error : {e}",
                "error"
            )

    def get_max_regional_flights_by_continent_v1(self, df: DataFrame) -> None:  
        """
        This function retrieves the airline with the most regional flights by continent from a given
        DataFrame.
        
        :param df: The input DataFrame containing flight data
        :type df: DataFrame
        :return: a logger message with the result of the query to find the airline with the most
        regional flights by continent, or an error message if an exception occurs.
        """
        try:      
            # get by filter flights actif or other(never take scheduled flights) and departure_continent == arrival_continent
            regional_flights = df.filter((col("departure_continent") == col("arrival_continent")) & (col("flight_status") != "scheduled"))
            regional_flights.cache()
            # if regional_flights is empty, I take all flights with only this condition : departure_continent == arrival_continent
            if regional_flights.count() <= 0:
                print("I can not get airline with the most regional flights active and departure_continent = arrival_continent")
                logging.warning("I can not get airline with the most regional flights active and departure_continent == arrival_continent")
                regional_flights = df.filter((col("departure_continent") == col("arrival_continent")))

            # Group the data by "departure_continent" and "airline_name" to see how many regional flights to every airline 
            regional_flights_count = regional_flights.groupBy("departure_continent", "airline_name") \
                    .agg(count("*").alias("flight_count"))

            # Group the data by "departure_continent" and find the maximum flight count by departure_continent 
            max_regional_flights_count = regional_flights_count.groupBy("departure_continent") \
                    .agg(max("flight_count").alias("max_flight"))

            # Join the "max_regional_flights_count" dataFrame with the "regional_flights_count" dataFrame, to get the list airline name corresponding to the maximum flight count
            max_regional_flights_by_continent = max_regional_flights_count.join(regional_flights_count, ["departure_continent"], "inner") \
                    .filter(col("flight_count") == col("max_flight"))

            # Group data by "departure_continent" and collect values "max_flight" corresponding
            max_regional_flights_by_continent = max_regional_flights_by_continent.groupBy("departure_continent", "max_flight") \
                    .agg(collect_list("airline_name").alias("airline_names"))
                    
            regional_flights.unpersist()
            return logger_message(
                max_regional_flights_by_continent,
                "[REQUEST] -> Airline with the most regional flights by continent version 1 is done."
            )
        except Exception as e:
            return logger_message(
                self.spark.createDataFrame([], StructType([])),
                f"Error to get airline with the most regional flights by continent version 1. See function get_max_regional_flights_by_continent_v1. See error : {e}",
                "error"
            )

    def get_max_regional_flights_by_continent_v2(self, df: DataFrame) -> None:
        """
        The function returns a DataFrame with the airline name that has the most regional flights by
        continent, with missing values replaced with "N/A".
        
        :param df: A DataFrame containing information about regional flights by continent and airline
        names
        :type df: DataFrame
        :return: a DataFrame containing the airline with the most regional flights by continent, with
        the "airline_names" column dropped and the "max_airline_name" column added. The function also
        logs a message indicating that the request is done.
        """
        try:
            # max_regional_flights_by_continent = df.withColumn("max_airline_name", array_min(col("airline_names")))
            max_regional_flights_by_continent = df.withColumn("max_airline_name", when(col("airline_names").isNull() | isnan(col("airline_names")), "N/A") \
                                                    .otherwise(array_min(col("airline_names"))))
            
            max_regional_flights_by_continent = max_regional_flights_by_continent.drop("airline_names")

            return logger_message(
                max_regional_flights_by_continent,
                "[REQUEST] -> Airline with the most regional flights by continent version 2 is done."
            )
        except Exception as e:
            return logger_message(
                self.spark.createDataFrame([], StructType([])),
                f"Error to get airline with the most regional flights by continent version 2. See function get_max_regional_flights_by_continent_v2. See error : {e}",
                "error"
            )

    def get_flights_with_max_duration(self, df: DataFrame) -> None:
        """
        This function takes a DataFrame of flight data and returns the flights with the maximum
        duration, along with their relevant information.
        
        :param df: The input DataFrame containing flight data
        :type df: DataFrame
        :return: a DataFrame containing flights with the maximum duration, along with their flight
        number, arrival and departure airports, and scheduled departure and arrival times. The function
        also logs a message indicating whether it has completed successfully or if there was an error.
        """
        # df.cache()
        try:
            # Found max duration
            # max_duration = df.agg(max("duration").alias("max_duration")).select("max_duration").first()[0]
        
            flights_with_max_duration = df.withColumn("max_duration", max("duration")).filter(col('max_duration') == col("duration")).select("duration_in_hour", "flight_number", "arrival_airport", 'departure_airport', "departure_scheduled", "arrival_scheduled")
            # df.unpersist()
            return logger_message(
                flights_with_max_duration,
                "[REQUEST] -> Flights with max duration is done."
            )  
        except Exception as e:
            return logger_message(
                self.spark.createDataFrame([], StructType([])),
                f"Error to get flights with max duration. See function get_flights_with_max_duration. See error : {e}",
                "error"
            )
        

    def get_average_flight_duration(self, df: DataFrame) -> None:
        """
        This function calculates the average flight duration for each continent using a window partition
        and returns a logger message.
        
        :param df: The input DataFrame containing flight data with columns "departure_continent" and
        "duration"
        :type df: DataFrame
        :return: a logger message with the result of calculating the average flight duration for each
        continent in the input DataFrame. If there is an exception, it returns a logger message with the
        input DataFrame and the error message.
        """
        try:
            windowSpec = Window.partitionBy("departure_continent")

            # Calculate average duration for each continent
            average_flight_duration = df.withColumn("average_duration_seconds", avg("duration").over(windowSpec)) \
                                        .withColumn("average_duration", from_unixtime(col("average_duration_seconds"), "HH:mm:ss")) \
                                        .drop("average_duration_seconds")

            return logger_message(
                average_flight_duration,
                "[REQUEST] -> Average flight duration is done."
            )
        except Exception as e:
            return logger_message(
                self.spark.createDataFrame([], StructType([])),
                f"Error to get average flight duration. See function get_average_flight_duration. See error : {e}",
                "error"
            )

    def get_number_flights_by_departure_arrival_airport(self, df: DataFrame) -> tuple:
        """
        This function retrieves the departure and arrival airports with the most flights from a given
        DataFrame.
        
        :param df: The input DataFrame containing flight data
        :type df: DataFrame
        :return: a tuple containing two DataFrames: number_flights_by_departure_airport and
        number_flights_by_arrival_airport.
        """
        try:
            flight_status = df.filter(col('flight_status') == "active")

            windowSpecDeparture = Window.partitionBy("departure_airport").orderBy(desc("count"))
            windowSpecArrival = Window.partitionBy("arrival_airport").orderBy(desc("count"))

            # Calculate number of flights by departure airport and take the first row
            number_flights_by_departure_airport = df.groupBy('departure_airport') \
                                                    .count() \
                                                    .withColumn("rank", dense_rank().over(windowSpecDeparture)) \
                                                    .filter(col("rank") == 1) \
                                                    .drop("rank")

            # Calculate number of flights by arrival airport and take the first row
            number_flights_by_arrival_airport = df.groupBy('arrival_airport') \
                                                    .count() \
                                                    .withColumn("rank", dense_rank().over(windowSpecArrival)) \
                                                    .filter(col("rank") == 1) \
                                                    .drop("rank")

            if flight_status.count() <= 0:
                print("I can not retrieve airports with the most active flights")
                logging.warning("I can not retrieve airports with the most active flights")

            print(f"Departure Airport with the most flights: {number_flights_by_departure_airport.first()}.")
            print(number_flights_by_departure_airport.show())
            logging.info(f"Departure Airport with the most flights: {number_flights_by_departure_airport.first()}.")
            
            print(f"Arrival Airport with the most flights: {number_flights_by_arrival_airport.first()}.")
            logging.info(f"Arrival Airport with the most flights: {number_flights_by_arrival_airport.first()}.")
            print(number_flights_by_arrival_airport.show())

            logging.info("[REQUEST] -> Departure and Arrival Airport with the most flights is done.")
            return number_flights_by_departure_airport, number_flights_by_arrival_airport
        except Exception as e:
            logging.error(f"Error to get number of flights by departure and arrival airport. See function get_number_flights_by_departure_arrival_airport. See error : {e}")
            return self.spark.createDataFrame([], StructType([])), self.spark.emptyDataFrame

    def get_top3_airplanes_used_by_country_dataframe(self, df: DataFrame) -> None:
        """
        This function retrieves the top 3 airplanes used by country from a given DataFrame.
        
        :param df: The parameter 'df' is a DataFrame that contains information about flights, including
        the departure city, airline name, aircraft registration, and other details. The function is
        designed to group this data by departure city, airline name, and aircraft registration, and then
        return the top 3 airplanes used by each country
        :type df: DataFrame
        :return: a DataFrame containing the top 3 airplanes used by country, grouped by departure city,
        airline name, and aircraft registration. If there is an error, it returns an empty DataFrame and
        logs the error message.
        """
        try:
            aircraft_exist = df.where(col('aircraft').isNull()).count()
            if not aircraft_exist:
                print("I can not retrieve top 3 airplanes used by country")
                logging.info("I can not retrieve top 3 airplanes used by country")
                return self.spark.emptyDataFrame
        
            # Group by 'departure_city', "airline_name", 'aircraft_registration' , to count number flights
            grouped_dataframe = df.groupBy('departure_city', "airline_name", 'aircraft_registration').count().orderBy(desc("count"))

            # Assign rank to every departure_city in function number flights
            window_spec = Window.partitionBy('departure_city').orderBy(col('count').desc())
            ranked_dataframe = grouped_dataframe.withColumn('rank', row_number().over(window_spec))

            # Filter to get only top 3 airline
            top3_airplanes_used_dataframe = ranked_dataframe.filter(col('rank') <= 3)

            # Sort by "departure_city" and 'rank'
            top3_airplanes_used_by_country_dataframe = top3_airplanes_used_dataframe.orderBy("departure_city", 'rank')
            
            print(f'Top 3 airplanes used by country: \n {top3_airplanes_used_by_country_dataframe.show()}')
            logging.info(f'Top 3 airplanes used by country: \n {top3_airplanes_used_by_country_dataframe.show()}')
            
            return logger_message(
                top3_airplanes_used_by_country_dataframe,
                "[REQUEST] -> Top 3 airplanes used by country is done."
            )
        except Exception as e:
            return logger_message(
                self.spark.createDataFrame([], StructType([])),
                f"Error to get top 3 airplanes used by country. See function get_top3_airplanes_used_by_country_dataframe. See error : {e}",
                "error"
            ) 
       
    def get_airport_with_max_diff(self, df: DataFrame) -> None:
        try:
            # Number of outgoing flights per departure airport
            outgoing_flights = df.groupBy('departure_airport').count().withColumnRenamed('count', 'outgoing_count')

            # Number of incoming flights per arrival airport
            incoming_flights = df.groupBy('arrival_airport').count().withColumnRenamed('count', 'incoming_count')

            # Calculate the difference between outgoing_flights and incoming_flights, and take the absolute value
            airport_flights_diff = outgoing_flights.join(incoming_flights, outgoing_flights['departure_airport'] == incoming_flights['arrival_airport'], 'outer') \
                .withColumn('flight_diff', abs(col('outgoing_count') - col('incoming_count')))

            windowSpec = Window.orderBy(col('flight_diff').desc())

            # Find the airport with the maximum difference
            airport_with_max_diff = airport_flights_diff.withColumn("rank", row_number().over(windowSpec)) \
                .filter(col("rank") == 1) \
                .drop("rank")

            print(f"The airport with the largest difference between outgoing and incoming flights is: {airport_with_max_diff.first()}")
            print(airport_with_max_diff.show())
            logging.info(f"The airport with the largest difference between outgoing and incoming flights is: {airport_with_max_diff.first()}")

            return logger_message(
                airport_with_max_diff, 
                "[REQUEST] -> Airport with max diff is done."
            )
        except Exception as e:
            return logger_message(
                self.spark.createDataFrame([], StructType([])),
                f"Error to get airport with max diff. See function get_airport_with_max_diff. See error : {e}",
                "error"
            )
    
    def runner(self, df: DataFrame):
        """
        The function runs various methods to extract insights from a given DataFrame.
        
        :param df: The input DataFrame that contains flight data
        :type df: DataFrame
        """
        # spark = ti.xcom_pull(key='spark_session')

        # data_extracted = get_last_parquet_file(self.directory)
        
        # df = spark.read.parquet(data_extracted)
        
        # self.describe_dataframe(df)
        
        self.get_airline_with_more_flights(df)
        
        res = self.get_max_regional_flights_by_continent_v1(df)
        self.get_max_regional_flights_by_continent_v2(res)
        
        self.get_flights_with_max_duration(df)
        self.get_average_flight_duration(df)
        self.get_number_flights_by_departure_arrival_airport(df)
        self.get_top3_airplanes_used_by_country_dataframe(df)
        self.get_airport_with_max_diff(df)
        