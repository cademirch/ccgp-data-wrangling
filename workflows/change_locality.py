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


def grab_tsv_row(row, collection):
    sample = row["*sample_name"]
    loca = row["Locality Description"]
    query = {"*sample_name": sample}

    document_to_update = collection.find_one(query)
    if document_to_update:
        print(f'Found {sample}')
        update_query = {"$set": {"Locality Description": loca}}
        result = collection.update_one(query, update_query)
        print(f'Updated {sample}')
        print('')

    else:
        print(f'{sample} not found... Skipping.')
        print('')

def process_tsv(tsv, collection):
    with open(tsv, 'r') as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            grab_tsv_row(row, collection)


def main():

    client = get_mongo_client()
    db = client["ccgp_dev"]
    collection = db["sample_metadata"]

    tsv_file_path = f'CCGP_Metadata_Submission_Azolla_Rothfels.tsv'

    process_tsv(tsv_file_path, collection)


if __name__ == "__main__":

    main()



