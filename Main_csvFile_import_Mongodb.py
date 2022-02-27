import logging
from MongoDb_data_DML_manipulations import csv_file_import


logging.basicConfig(filename="C:/Users/vmuth/Downloads/csv_mongodb_import.log", level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

csv_path = "C://Users//vmuth//anaconda3//python-local-files/carbon_nanotubes.csv"
db_name = "CSV-to-Json-Import"
coll_name = "Json-Payload-Import"
connect_string = "mongodb+srv://Mongodb:Mongodb@cluster0.gbddl.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

class_object = csv_file_import(connect_string, csv_path, db_name, coll_name)
class_object.data_count_validation()
class_object.dml_insert()
class_object.find()
class_object.dml_update()
class_object.dml_delete()