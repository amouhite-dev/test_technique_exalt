import os
import sys
# add directory source in PATH
fileDirectory = "/".join(os.path.realpath(__file__).split("/")[:-3]) # in prod
sys.path.append(fileDirectory)

from decouple import config

from utils.logger import logging

from pymongo import MongoClient

USER_DB= config("USER_DB")
PASSWORD_DB= config("PASSWORD_DB")
HOST_DB= config("HOST_DB")
PORT_DB= config("PORT_DB")
DATABASE= config("DATABASE")
COLLECTION = config("COLLECTION")

SQL_NO_QUERY =  "NO QUERY CREATE"
QUERY_NOTIFY = "QUERIES SAVED"

def connect_client() -> MongoClient:
    uri = f"mongodb://{USER_DB}:{PASSWORD_DB}@{HOST_DB}:{PORT_DB}/{DATABASE}?authSource=admin&authMechanism=SCRAM-SHA-256"
    print("uri : ", uri)
    client = MongoClient(uri)
    logging.info(client.list_database_names())
    logging.info(client[DATABASE].list_collection_names())
    return client
  
def deconnect_client(client: MongoClient) -> None:
    """
    This function closes the connection to a MongoDB client and logs a message indicating whether
    the operation was successful or not.
    
    :param client: MongoClient object representing the connection to a MongoDB database
    :type client: MongoClient
    """
    try:
        client.close()
        logging.info("The connection to client MongoDB is closed.")
    except Exception as e:
        logging.error(f"ERROR when I deconnect to client MongoDB. See error [{e}].")

def create_collection(collection, database: object) -> None:
    """
    This function creates collections in a MongoDB database if they do not already exist.
    
    :param database: The database parameter is an instance of the object class from the PyMongo
    library. It is used to connect to a MongoDB server and perform database operations
    :type database: object
    """
    try:
        if collection in database.list_collection_names():
            logging.info(f"The '{collection}' collection already exists.")
        else:
            collection = database[collection]
            collection.insert_one({})
            logging.info(f"The '{collection}' collection is created.")
    except Exception as e:
        logging.error(f"ERROR when I create collection '{collection}'. See error [{e}].")

def create_or_connect_to_database() -> None:
    """
    This function creates a MongoDB client and checks if a database exists, and if not, creates it.
    :return: a MongoClient object connected to a MongoDB database specified by the constant
    DATABASE.
    """
    # Create client MongoDB
    client = connect_client()
    # check if database exists
    if DATABASE in client.list_database_names():
        logging.info(f"The '{DATABASE}' database already exists.")
    else:
        # Create database
        database = client[DATABASE]
        logging.info(f"The '{DATABASE}' database is created.")
        create_collection(COLLECTION, database)
    return client
       
        
        
        
        
        