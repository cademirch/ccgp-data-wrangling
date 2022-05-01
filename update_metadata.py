from db import get_mongo_client
import pymongo
import parse
from pymongo.errors import BulkWriteError
from pprint import pprint
from datetime import datetime
from pathlib import Path
import pandas as pd
import os
import argparse
from gdrive import CCGPDrive
import sys
from collections import defaultdict


def update_metadata(db_client: pymongo.MongoClient):
    """Parses metadata sheets from minicore and non minicore sources and updates database."""
    db = db_client["ccgp"]
    collection = db["ccgp-samples"]
    parsed_metadatas = db["parsed_metadata_files"]
    ccgp_workflow_progress = db["ccgp_workflow_progress"]
    already_read = [doc.get("file_name") for doc in parsed_metadatas.find({})]
    drive = CCGPDrive()
    non_minicore_files = [
        item
        for item in drive.list_files_from_folder("Non-Minicore Submissions")
        if item["name"] not in already_read
    ]
    minicore_files = [
        item
        for item in drive.list_files_from_folder("Minicore Submissions")
        if item["name"] not in already_read
    ]
    if not non_minicore_files and not minicore_files:
        print("Nothing to be done.")
        return
    files_to_download = non_minicore_files + minicore_files
    drive.download_files(files_to_download)

    df = parse.get_big_df(
        minicore_files=minicore_files, non_minicore_files=non_minicore_files
    )
    project_ids = defaultdict(int)
    operations = []
    for _, row in df.iterrows():
        record = row.to_dict()
        project_ids[record["ccgp-project-id"]] += 1
        operations.append(
            pymongo.operations.UpdateOne(
                filter={"*sample_name": record["*sample_name"]},
                update={"$set": record},
                upsert=True,
            )
        )
    try:
        collection.bulk_write(operations)
    except BulkWriteError as bwe:
        pprint(bwe.details)
    print(project_ids)
    for p_id, counts in project_ids.items():
        ccgp_workflow_progress.update_one(
            filter={"project_id": p_id},
            update={"$addToSet": {"Metadata Recieved": (datetime.utcnow(), counts)}},
            upsert=True,
        )
    for file in files_to_download:
        name = file["name"]
        parsed_metadatas.update_one(
            filter={"file_name": name},
            update={"$set": {"file_name": name}},
            upsert=True,
        )

        Path(name).unlink()


def add_biosample_accessions(
    db_client: pymongo.MongoClient,
) -> None:
    """Reads attributes.tsv from BioSample submission to add biosample accessions to samples in database"""
    db = db_client["ccgp"]
    collection = db["ccgp-samples"]
    ccgp_workflow_progress = db["ccgp_workflow_progress"]
    parsed_attribute_files = db["parsed_attribute_files"]

    already_read = [doc.get("file_name") for doc in parsed_attribute_files.find({})]
    drive = CCGPDrive()
    to_process = [
        item
        for item in drive.list_files_from_folder("NCBI Biosample Attributes")
        if item["name"] not in already_read
    ]
    if not to_process:
        print("Nothing to be done.")
        return

    drive.download_files(to_process)

    to_process = [Path(item["name"]) for item in to_process]
    for tsv_file in to_process:
        print(tsv_file)
        df = pd.read_csv(tsv_file, sep="\t", header=0)
        operations = []
        for _, row in df.iterrows():
            record = row.to_dict()
            operations.append(
                pymongo.operations.UpdateOne(
                    filter={"*sample_name": record["sample_name"]},
                    update={
                        "$set": {
                            "biosample_accession": record["accession"],
                        }
                    },
                    upsert=False,
                )
            )
            print(f"Added {record['accession']} to {record['sample_name']}")
        project_id = collection.find_one(
            filter={"*sample_name": record["sample_name"]}
        )["ccgp-project-id"]

        ccgp_workflow_progress.update_one(
            filter={"project_id": project_id},
            update={
                "$set": {
                    "biosamples_created": datetime.fromtimestamp(
                        os.path.getmtime(tsv_file)
                    )
                }
            },
            upsert=True,
        )

    try:
        collection.bulk_write(operations)
    except pymongo.errors.BulkWriteError as bwe:
        pprint(bwe.details)

    for file in to_process:
        parsed_attribute_files.update_one(
            filter={"file_name": file.name},
            update={"$set": {"file_name": file.name}},
            upsert=True,
        )
        file.unlink()


def main():
    db = get_mongo_client()
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(
        dest="command", title="subcommands", description="valid subcommands"
    )
    metadata = subparser.add_parser("metadata", description="Update metadata")
    attributes = subparser.add_parser(
        "attributes", description="Update biosample accessions"
    )
    both = subparser.add_parser("both", description="Update both")
    args = parser.parse_args()

    if args.command == "metadata":
        update_metadata(db)
    elif args.command == "attributes":
        add_biosample_accessions(db)
    elif args.command == "both":
        update_metadata(db)
        add_biosample_accessions(db)


if __name__ == "__main__":
    main()
