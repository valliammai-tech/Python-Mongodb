import logging
import pymongo
import Load_Csv_into_Mongodb

class csv_file_import:
    def __init__(self,connect_string, csv_path, db_name, coll_name):
        self.connect_string = connect_string
        self.csv_path = csv_path
        self.db_name = db_name
        self.coll_name = coll_name
        client = pymongo.MongoClient(self.connect_string)
        db = client[self.db_name]
        self.coll = db[self.coll_name]
            

    def data_count_validation(self):
        try:
            count=Load_Csv_into_Mongodb.mongoimport(self.connect_string, self.csv_path, self.db_name, self.coll_name)
        except Exception as e:
            logging.warning("mongodb connection error occurred. Please check")
            logging.error(e)
        else:
            if count == 10721:
                logging.info("count of inserted documents verified and it looks good")
                return True
            else:
                logging.info("{} is not the expected count. Please check insertion operation".format(count))
                return False
    
    def dml_insert(self):
        record_to_insert = {"_id":"new_record_inserted","Chiral indice n":2,"Chiral indice m":1,"Initial atomic coordinate u":"0,679005","Initial atomic coordinate v":"0,701318","Initial atomic coordinate w":"0,017033","Calculated atomic coordinates u'":"0,721039","Calculated atomic coordinates v'":"0,730232","Calculated atomic coordinates w'":"0,017014"}
        self.coll.insert_one(record_to_insert)
        logging.info("new document inserted using dml_insert method. Please verify..")
    
    def find(self):
        find_object = self.coll.find({"Chiral indice n":{"$gte":7}})
        for i in find_object:
            logging.info("find_object {}".format(i))
    
    def dml_update(self):
        self.coll.update_many({"Chiral indice n":7},{"$set":{"Chiral indice n":77}})
        logging.info("Documents updated for chiral indice n value using dml_update. Please validate.")

    def dml_delete(self):
        self.coll.delete_many({"Calculated atomic coordinates u'":"0,721039"})
        logging.info("documents with calculated atomic coordinated u as 0,721039 deleted. Please validate.")
    
    
    