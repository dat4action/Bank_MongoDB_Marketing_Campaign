from pymongo import MongoClient
import csv


def connect_to_mongodb(database):
    """Connects to a Mongodb client

    Args:
        database (str): name of the database of the client

    Returns:
        db: MongoDB
    """
    client = MongoClient("mongodb://localhost:27017")
    db = client[database]
    print(f"db: {db}")
    print(f"client: {client}")
    return db


def import_csv_to_collection(csv_file, collection_name, db):
    """Converts a csv to a Mongo DB collection

    Args:
        csv_file (str): path name of the csv file
        collection_name (str): name of the collection to be created
        db (db): MongoDB database object
    """
    collection = db[collection_name]
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        records = []
        for row in reader:
            records.append(row)
        collection.insert_many(records)
    print(f"Imported {csv_file} into the {collection_name} collection.")