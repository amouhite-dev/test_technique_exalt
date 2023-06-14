import os
import sys
# add directory source in PATH
fileDirectory = "/".join(os.path.realpath(__file__).split("/")[:-2])
print(f"directory source {fileDirectory}")
sys.path.append(fileDirectory)

from decouple import config
from utils.logger import logging
from etl.request_data import RequestData
from etl.extraction import ExtractionData
from etl.treatment_data import TreatmentData
from utils.commons import  pass_to_dict, create_session_spark
from database.actions import run_session_transaction, save_data_by_collection
from database.connector import create_or_connect_to_database, deconnect_client

def run():
    # config to get data
    url_flights = config('URL_FLIGHTS')
    params = {
    'access_key': config('ACCESS_KEY'),
    }
    
    client = create_or_connect_to_database()
    session = run_session_transaction(client)
    spark = create_session_spark()
    database = client[config('DATABASE')]
    
    extract = ExtractionData(url_flights, params, "flights")
    df_extracted = extract.runner(spark)
    
    logging.info("Extraction data is done !!!")
    
    transform = TreatmentData()
    df_transformed = transform.runner(df_extracted)
    
    logging.info("Transformation data is done !!!")
    
    flights_to_save = pass_to_dict(df_transformed)
    save_data_by_collection(database, session, config('COLLECTION'), flights_to_save)
    
    request = RequestData(spark)
    request.runner(df_transformed)
    
    deconnect_client(client)
    spark.stop()
        
if __name__ == "__main__":
    run()   