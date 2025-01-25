from pymongo import MongoClient, DESCENDING,errors
from pymongo.errors import BulkWriteError, PyMongoError
import json
import logging
import os
from datetime import timezone,datetime
from bson.objectid import ObjectId



logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


def deployMetadataCollection(uri:str,dbName:str,collectionName:str):

    # Connect to the MongoDB instance using the connection string
    client = MongoClient(uri)
    db = client[dbName]
    dataMappingCollection = db[collectionName]

    
    #delete all documents in the collection
    dataMappingCollection.delete_many({})
    
    logging.debug('Loading the data from the datatypesConvert.json file')
    
    # Get the absolute path of the directory where mongodbHelper.py is located
    currentDir = os.path.dirname(__file__)
    
    # Construct the path to the JSON file in the static folder
    mappingFilePath = os.path.join(currentDir, '..', 'static', 'datatypesConvert.json')

    # Open and load the JSON file
    with open(mappingFilePath, "r") as f:
        data = json.load(f)


    # Insert the data into the collection

    try:
        result = dataMappingCollection.insert_many(data)
        l=len(result.inserted_ids)
        logging.debug(f"Inserted {l} documents successfully.")
    
    except BulkWriteError as bwe:
        l=[]
        logging.debug("Error inserting documents:", bwe.details)
    
    
    return l





def getExtractionConfig(uri:str, dbName:str, collectionName:str, dataSourceId:str)->dict:
    '''
    description: get the latest extraction config for a given data source id
    params: dataSourceId:str - the data source id
    returns: 
            if the document was found in the mongoDB, retun thr document, else return None
    raise: raise 
    '''
    
    logging.debug(f'fetching the extractionConfig JSON for dataSourceId- {dataSourceId} databa from mongoDB')
    # Connect to the MongoDB instance using the connection string
    try:
        client = MongoClient(uri)

        # Get a reference to the specified database
        db = client[dbName]
        config_collection = db[collectionName]

        
        query = {"_id": ObjectId(dataSourceId)}
        sort_by = [("submitTimestampUTC", DESCENDING)]
        extractionConfig = config_collection.find_one(query, sort=sort_by)
        return extractionConfig
    
    except Exception as e:
        logging.error(f'Error while trying to fetch the extractionConfig document for _id = {dataSourceId}')
        raise Exception(f"An error occurred during MongoDB operation: {e}")
    


def deleteMongoDB(uri:str, dbName:str)->dict:
    logging.debug(f'dropping MongoDB {dbName}')
    try:
        client = MongoClient(uri)
        client.drop_database(dbName)
        logging.debug(f'MongoDB {dbName} deleting successfully')
    except PyMongoError as e:
        logging.error(f"An error occurred while tring to drop MongoDB {dbName}: {e}")
        



def insertExtractionConfig(uri:str, dbName:str, collection:str,extractionConfig:dict):
    logging.debug(f'inserting extractionConfig account = {extractionConfig["accountID"]} , datasourceName = {extractionConfig["datasourceName"]} to mongoDB  -  {dbName}')
    try:
        # Connect to the MongoDB server
        client = MongoClient(uri)
        
        # Access the specified database and collection
        db = client[dbName]
        collection = db[collection]

        extractionConfig['submitTimestampUTC'] = datetime.now(timezone.utc)
        
        
        # Insert the document into the collection
        result = collection.insert_one(extractionConfig)
        
        # Return the result of the insertion
        return {"success": True, "_id": str(result.inserted_id)}
    
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        # Close the connection to the server
        client.close()






