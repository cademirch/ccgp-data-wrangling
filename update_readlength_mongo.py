import sys
import pandas as pd
from utils.db import get_mongo_client

PERFORM_MONGODB_UPDATE = True
def update_mongoDB(read_lengths, output_log):

    client = get_mongo_client()
    db = client["ccgp_dev"]
    collection = db["reads"]

    missing_files = []
    found_files = []
    for i, row in read_lengths.iterrows():
        file = row["Sample"] + ".fastq.gz"

        query = {"file_name": file}
        update_val = {"$set": {"sequence_length": row['FastQC_mqc-generalstats-fastqc-total_sequences']}}

        if PERFORM_MONGODB_UPDATE:
            result = collection.update_one(query, update_val)
            
            if result.matched_count == 0:
                missing_files.append(f"Entry not found for {file}: {row['FastQC_mqc-generalstats-fastqc-total_sequences']}")
            elif result.matched_count == 1:
                found_files.append(f"Found {file}: {row['FastQC_mqc-generalstats-fastqc-total_sequences']}")
        else:
            missing_files.append(f"Simulated update for {file}: {row['FastQC_mqc-generalstats-fastqc-total_sequences']}")


    successful_counts = f'# of Successful Matches = {len(found_files)}'
    unmatched_counts = f'# of Failed Matches = {len(missing_files)}'

    with open(output_log, "a") as file:
        file.write(successful_counts + "\n")
        file.write(unmatched_counts + "\n")
        file.write('' + "\n")
        file.write('' + "\n")
        file.write("Missing File Log:" + "\n")
        for entry in missing_files:
            file.write(entry + "\n")
        file.write('' + "\n")
        file.write('' + "\n")
        file.write("Matched File Log:" + "\n")
        for found_entry in found_files:
            file.write(found_entry + "\n")



if __name__ == "__main__":
    input_lengths = snakemake.params["reads"]
    output_log = snakemake.params["log"]


    read_lengths = pd.read_csv(input_lengths)

    update_mongoDB(read_lengths, output_log)