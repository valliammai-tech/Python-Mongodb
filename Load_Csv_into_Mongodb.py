import pymongo
import pandas as pd
import json
import logging

def mongoimport(connect_string,csv_path, db_name, coll_name):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """
    try:
        client = pymongo.MongoClient(connect_string)
        db = client[db_name]
        existing_db = client.list_database_names()
        if db in existing_db:
            logging.warning("{} already exists".format(db))
        coll = db[coll_name]
        existing_coll = db.list_collection_names()
        if coll in existing_coll:
            logging.warning("{} already created in mongodb. if you proceed, duplicate data will be inserted.")        
        data = pd.read_csv(csv_path, sep=";")
        payload = json.loads(data.to_json(orient='records'))
        coll.insert_many(payload)
    except FileNotFoundError:
        logging.critical("csv file to import is not available in csv_path {}".format(csv_path))
        return ("csv file to import is not available in csv_path")
    except Exception as e:
        logging.error(e)
        logging.warning("Exception occurred and csv file import to mongodb process terminated")
        return "Exception occurred and csv file import to mongodb process terminated"
    else:
        c=coll.count_documents({})
        logging.info("csv file data successfully imported into mongodb and count of documents imported is {}".format(c))
        return coll.count_documents({})        
