import pandas as pd
import pymongo
from pymongo.errors import BulkWriteError
import re
import math
from datetime import datetime
import os
import sys
import argparse

'''
First script to run when Ryan sends updated coordinates.
Updates coordinate information on mongo. Make sure to put sheet in "coordinate_sheets" before running script.

Input: Excel spreadsheet (.xlsx)

'''

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)
from utils.db import get_mongo_client
sys.path.remove(parent_dir)


def read_mongo_and_excelsheet(xlsx_path):
    
    script_directory = os.path.dirname(os.path.abspath(__file__))
    relative_path = os.path.join(script_directory, 'coordinate_sheets', xlsx_path)
    df = pd.read_excel(relative_path)
    client = get_mongo_client()
    db = client["ccgp_dev"]
    collection = db["sample_metadata"]

    return collection, df


def read_coordiante_data_csv(xlsx_path, mongo_data, df):
    operations = []
    df["protected_coords"] = df["protected_coords"].astype(str)
    df["protected_coords"] = df["protected_coords"].str.upper()
    df["exclude"] = df["exclude"].astype(str)
    df["exclude"] = df["exclude"].str.upper()

    samples_with_no_metadata = []

    for i, row in df.iterrows():
        record = row.to_dict() # record of the excel spreadsheet data.
        sample_name = str(record["*sample_name"])
        ccgp_project_id = row["ccgp-project-id"]
        #if sample_name in mongo_data["*sample_name"]:
        if mongo_data.count_documents({"*sample_name": sample_name}) > 0:
            operations.append(
                pymongo.operations.UpdateOne(  # type: ignore
                    filter={"*sample_name": sample_name, "ccgp-project-id": ccgp_project_id},
                    update={"$set": record},
                    upsert=True,
                )
            )
            #print(sample_name)
        else:
            samples_with_no_metadata.append(sample_name)

    samp_len = len(samples_with_no_metadata)
    try:
        mongo_data.bulk_write(operations)   
        print(f'{samp_len} samples could not be found on Mongo: {samples_with_no_metadata}')
    except BulkWriteError as bwe:
        print(bwe)
        print()

    


def main():
    parser = argparse.ArgumentParser(description="Extract metadata for a specific project (ccgp-project-id) and data type (plant, vertebrate, invertebrate)")  # Add file type later... aka tsv or sra.
    parser.add_argument("file_name", type=str, help="Specify the .xlsx file")  # Initializes 'project_name' Argument.
    args = parser.parse_args()



    mongo_data, df = read_mongo_and_excelsheet(args.file_name)
    read_coordiante_data_csv(args.file_name, mongo_data, df)



if __name__ == "__main__":
    main()