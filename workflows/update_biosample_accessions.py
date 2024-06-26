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


already_processed = []

def grab_tsv_row(row, collection, output_file):
    accession = row["biosample_accession"]
    bioproject = row["bioproject_accession"]
    filename = row["filename"]
    query = {"files": filename}

    sample = row["sample_name"]
    query = {"*sample_name": sample}

    document_to_update = collection.find_one(query)
    if document_to_update:
        print(f'Found {accession}')
        if accession not in already_processed:
            update_query = {"$set": {"ncbi_accession_id": accession, "ncbi_bioproject": bioproject}}
            result = collection.update_one(query, update_query)
            already_processed.append(accession)
            print(f'Updated {accession}')
            print('')
        else:
            print(f'{accession} already accounted for... Skipping.')
            print('')
    else:
        print(f'{filename} not found in Mongo query... Skipping.')
        print('')
        with open(output_file, 'a') as f:
            f.write(f"{filename}\t{accession}\n")


def process_tsv(tsv, collection, output_file):
    with open(tsv, 'r') as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            grab_tsv_row(row, collection, output_file)


def main():
    # tsv_file_path = snakemake.params["metadata_file"]
    # output_file = snakemake.params["output"]
    client = get_mongo_client()
    db = client["ccgp_dev"]
    collection = db["sample_metadata"]
    reads_collection = db["reads"]

    accession_sheets_folder = "update_accession_sheets"
    file_names_original = os.listdir(accession_sheets_folder)
    file_names = [file_name.split(".tsv")[0] for file_name in file_names_original if file_name.endswith(".tsv")]
    print(file_names)
    for file_name in file_names:
        print(f'starting {file_name}')

        tsv_file_path = f'{accession_sheets_folder}/{file_name}.tsv'
        output_file = f'accession_update_errors/{file_name}.txt'

        process_tsv(tsv_file_path, collection, output_file)
        print('')
        print("########################")
        print(f'finished {file_name}')
        print("########################")
        print('')
        time.sleep(5)

if __name__ == "__main__":

    main()



