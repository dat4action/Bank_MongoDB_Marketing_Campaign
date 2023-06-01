from pymongo import MongoClient
from src.database import import_csv_to_collection, connect_to_mongodb
import os
import csv

from pymongo import MongoClient
import csv
import pytest


@pytest.fixture(scope='class')
def test_database():
    # Create a test database and collection
    database_name = 'test_database'
    collection_name = 'test_collection'
    client = MongoClient('mongodb://localhost:27017')
    db = client[database_name]
    collection = db[collection_name]

    yield db

    # Clean up the test database and collection
    client.drop_database(database_name)


class TestDatabase:
    def test_connect_to_mongodb(self, test_database):
        # Test the connect_to_mongodb function
        db = connect_to_mongodb(test_database.name)
        assert db == test_database

    def test_import_csv_to_collection(self, test_database, tmp_path):
        # Create a temporary CSV file
        csv_data = [
            {'name': 'John', 'age': '25'},
            {'name': 'Jane', 'age': '30'}
        ]
        csv_file = tmp_path / 'test.csv'
        with open(csv_file, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['name', 'age'])
            writer.writeheader()
            writer.writerows(csv_data)

        # Call the import_csv_to_collection function with the test_database
        import_csv_to_collection(csv_file, 'test_collection', test_database)

        # Verify that the records were inserted into the collection
        expected_records = [{'name': 'John', 'age': '25'}, {'name': 'Jane', 'age': '30'}]
        actual_records = list(test_database.test_collection.find({}, {'_id': 0}))  # Exclude the _id field
        expected_records_no_id = [{k: v for k, v in record.items() if k != '_id'} for record in expected_records]
        assert actual_records == expected_records_no_id