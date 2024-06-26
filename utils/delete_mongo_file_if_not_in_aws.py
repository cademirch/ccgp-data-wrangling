import os
import pymongo
from dotenv import load_dotenv
from db import get_mongo_client

'''
This script reads a text file containing filenames of missing files in the MongoDB "reads" collection. It connects to MongoDB,
filters and deletes documents from the "reads" collection based on the missing filenames, and reports the number of deleted documents.
Ensure the MongoDB connection configuration is set up correctly and the text file contains the filenames to be deleted.
'''


def read_missing_files(filename):
    with open(filename, "r") as file:
        return file.read().splitlines()


def mongo_data():
    client = get_mongo_client()
    db = client["ccgp_dev"]
    collection = db["reads"]
    return collection

def main():

    missing_filenames = read_missing_files("files_in_mongo_but_not_aws.txt")
    
    if not missing_filenames:
        print("No missing files found.")
        return
    
    collection = mongo_data()

    delete_result = collection.delete_many({"file_name": {"$in": missing_filenames}})
    

    print(f"Deleted {delete_result.deleted_count} documents.")
    
if __name__ == "__main__":
    main()
