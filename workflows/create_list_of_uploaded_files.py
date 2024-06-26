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

def grab_tsv_row(row, output_file):

    filename = row["filename"]
    filename2 = row['filename2']
    #query = {"files": filename}

    with open(output_file, 'a') as f:
        f.write(f"{filename}\n")
        f.write(f"{filename2}\n")


def process_tsv(tsv, output_file):
    with open(tsv, 'r') as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            grab_tsv_row(row, output_file)


def main():

    accession_sheets_folder = "update_accession_sheets"
    file_names_original = os.listdir(accession_sheets_folder)
    file_names = [file_name.split(".tsv")[0] for file_name in file_names_original if file_name.endswith(".tsv")]
    print(file_names)
    for file_name in file_names:
        print(f'starting {file_name}')

        tsv_file_path = f'update_accession_sheets/{file_name}.tsv'
        output_file = 'all_files_on_mongo.txt'

        process_tsv(tsv_file_path, output_file)
        print('')
        print("########################")
        print(f'finished {file_name}')
        print("########################")
        print('')
        time.sleep(2)

if __name__ == "__main__":

    main()



