from datetime import datetime
import traceback
from utils.logger import logging
import os
import sys
# add directory source in PATH
fileDirectory = "/".join(os.path.realpath(__file__).split("/")[:-3]) # in prod
sys.path.append(fileDirectory)

import pymongo
from decouple import config

SQL_NO_QUERY =  "NO QUERY CREATE"
QUERY_NOTIFY = "QUERIES SAVED"

DATABASE= config("DATABASE")
COLLECTION = config("COLLECTION")

def run_session_transaction(client: pymongo.MongoClient) -> object:
    session = client.start_session()

    # Start a transaction
    session.start_transaction()
    
    return session

def close_session_transaction(client: pymongo.MongoClient) -> object:
    client.end_session()

def save_data_by_collection(database, session, collection:object, data:dict) -> int:
    try:
        data = [dict(d, **{"DateCrawlingDate": datetime.now().strftime("%Y-%M-%d")}) for d in data]
        data = [dict(d, **{"CrawlingHour": datetime.now().strftime("%H:%M:%S")}) for d in data]
        database[collection].insert_many(data)
        # Commit the transaction
        session.commit_transaction()
        text_debug = f"{QUERY_NOTIFY} TO COLLECTION {collection}" + "\n\n"
        print(text_debug)
        logging.info(text_debug)
    except Exception as e:
        message_error_transaction(
            session,
            'Insertion failed ',
            e,
        )

def get_collection(client: pymongo.MongoClient, collection_name: str)-> object:
    database = client[DATABASE]
    return database[collection_name]

def message_error_transaction(session, message1, e):
    session.abort_transaction()
    text_exception = f"{message1}. \n\nSee error :\n[{e}]. See more {traceback.format_exc()}."
    logging.warning(text_exception)