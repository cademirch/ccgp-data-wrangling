import sys
sys.path.append(
    ".."
)
import pandas as pd
import re
import os
import csv
import time
from utils.db import get_mongo_client
from dotenv import load_dotenv
from os import getenv
import boto3

"""
This script is designed to intake processed NCBI metadata & assign it to the MongoDB "reads" collection.
"""

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
    objects = bucket.objects.all()
    aws_bucket = {obj.key: obj.size for obj in objects}
    return aws_bucket

already_processed = []

def grab_tsv_row(row, collection, output_file, aws_files):
    accession = row["accession"]
    bioproject = row["bioproject_accession"]
    #sample_name = row["sample_name"]
    filename = row["filename"]
    filename2 = row["filename2"]
    
    query = {"file_name": filename}
    query2 = {"file_name": filename2}

    document_to_update = collection.find_one(query)
    document_to_update2 = collection.find_one(query2)

    file_size = aws_files.get(filename, 0)
    file_size2 = aws_files.get(filename2, 0)

    if document_to_update:
        print(f'Found {accession} with filename {filename}')
        
        update_query = {"$set": {"srr_accession_id": accession, "ncbi_bioproject": bioproject, "uploaded_to_NCBI": "yes", "filesize": file_size}}
        result = collection.update_one(query, update_query)
        already_processed.append(accession)
        print(f'Updated {accession} with filename {filename}')
        print('')
    else:
        print(f'{filename} not found in Mongo query... Skipping.')
        print('')
        new_document = {
            "file_name": filename,
            #"*sample_name": sample_name,
            "srr_accession_id": accession,
            "ncbi_bioproject": bioproject,
            "uploaded_to_NCBI": "yes",
            "filesize": file_size
        }
        collection.insert_one(new_document)
        with open(output_file, 'a') as f:
            f.write(f"{filename}\t{accession}\n")

    if document_to_update2:
        print(f'Found {accession} with filename {filename2}')
        update_query = {"$set": {"srr_accession_id": accession, "ncbi_bioproject": bioproject, "uploaded_to_NCBI": "yes", "filesize": file_size2}}
        result = collection.update_one(query2, update_query)
        already_processed.append(accession)
        print(f'Updated {accession} with filename {filename2}')
        print('')
    else:
        print(f'{filename2} not found in Mongo query... Skipping.')
        print('')
        new_document2 = {
            "file_name": filename2,
            "srr_accession_id": accession,
            "ncbi_bioproject": bioproject,
            "uploaded_to_NCBI": "yes",
            "filesize": file_size2
        }
        collection.insert_one(new_document2)
        with open(output_file, 'a') as f:
            f.write(f"{filename2}\t{accession}\n")

def process_tsv(tsv, collection, output_file, aws_files):
    with open(tsv, 'r') as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            grab_tsv_row(row, collection, output_file, aws_files)


def main():
    aws_files = list_s3_bucket_objs()
    client = get_mongo_client()
    db = client["ccgp_dev"]
    collection = db["reads"]

    accession_sheets_folder = "update_accession_sheets"
    file_names_original = os.listdir(accession_sheets_folder)
    file_names = [file_name.split(".tsv")[0] for file_name in file_names_original if file_name.endswith(".tsv")]
    print(file_names)
    for file_name in file_names:
        print(f'starting {file_name}')

        tsv_file_path = f'{accession_sheets_folder}/{file_name}.tsv'
        output_file = f'srr_update_errors/{file_name}.txt'

        process_tsv(tsv_file_path, collection, output_file, aws_files)
        print('')
        print("########################")
        print(f'finished {file_name}')
        print("########################")
        print('')
        time.sleep(5)

if __name__ == "__main__":

    main()



