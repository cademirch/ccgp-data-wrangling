'''
This script collects all files associated with a CCGP Project from AWS and compares them to what is in the MongoDB "reads" collection
and collects any files that are in the "reads" collection, but ARE NOT in AWS.

The "reads" collection is used in the update_reads.py file update process as a reference for metadata matching and old/deleted files from
AWS that aren't accounted for will wrongfully be matched and stored as metadata, causing future pipelines (like running the workflow) to
fail.

Output:
    "files_in_mongo_but_not_aws.txt"

Access "delete_mongo_file_if_not_in_aws.py" to delete "files_in_mongo_but_not_aws.txt" contents from "reads" collection.

To run script: python3 check_if_mongo_files_in_aws.py

'''

import boto3
import pymongo
from dotenv import load_dotenv
from os import getenv
from db import get_mongo_client



def list_s3_bucket_objs():
    """Returns s3 objectCollection"""
    load_dotenv()
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=getenv("aws_access_key_id"),
        aws_secret_access_key=getenv("aws_secret_access_key"),
        endpoint_url=getenv("endpoint_url"),
    )
    bucket = s3.Bucket("ccgp")

    return bucket.objects.all()


def mongo_data():
    client = get_mongo_client()
    db = client["ccgp_dev"]
    collection = db["reads"]

    return collection

def main():

    s3_files = set([obj.key for obj in list_s3_bucket_objs()])
    db_collection = mongo_data()

    missing_files = []
    for document in db_collection.find({}, {"file_name": 1}):
        file_name = document.get("file_name")
        if file_name and file_name not in s3_files:
            missing_files.append(file_name)
    
    if missing_files:
        with open("files_in_mongo_but_not_aws.txt", "w") as file:
            file.write("\n".join(missing_files))
        print("Files found in Mongo, but not found in AWS have been logged in 'files_in_mongo_but_not_aws.txt'")
    else:
        print("All files in MongoDB exist in AWS S3.")

if __name__ == "__main__":
    main()