from utils.db import get_mongo_client
import pymongo
import utils.parse
from pymongo.errors import BulkWriteError
from pprint import pprint
from datetime import datetime
from pathlib import Path
import pandas as pd
import os
import argparse
from utils.gdrive import CCGPDrive
import sys
from collections import defaultdict
import traceback


def update_metadata(db_client: pymongo.MongoClient, force=False, file: str = None):
    """Parses metadata sheets from minicore and non minicore sources and updates database."""
    db = db_client["ccgp_dev"]
    collection = db["sample_metadata"]
    parsed_metadatas = db["parsed_metadata_files"]
    ccgp_workflow_progress = db["workflow_progress"]

    if force or file is not None:
        already_read = []
    else:
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

    non_minicore_files = [(f, "non_minicore") for f in non_minicore_files]
    minicore_files = [(f, "minicore") for f in minicore_files]
    files = non_minicore_files + minicore_files
    if not files:
        print("Nothing to be done.")
        return
    if file is not None:
        files = [item for item in files if file in item["name"]]
    metadata_ops = []
    workflow_prog_ops = []
    parsed_ops = []
    project_ids = defaultdict(int)
    try:
        for file, project_type in files:
            file_name = file["name"]
            print(file)
            drive.download_files(file)

            print("Processing file: " + "'" + file_name + "'")
            df = parse.finalize_df(parse.read_sheet(Path(file_name), project_type))
            print(f"Got a DataFrame of this shape: {df.shape}")
            # Replace NaNs with empty string b/c JSON for web dashboard cannot encode NaN
            # df = df.fillna("")
            for _, row in df.iterrows():
                record = row.to_dict()
                project_ids[record["ccgp-project-id"]] += 1
                print(
                    f"Procesed sample {record['*sample_name']} in project {record['ccgp-project-id']}"
                )
                metadata_ops.append(
                    pymongo.operations.UpdateOne(
                        filter={"*sample_name": record["*sample_name"]},
                        update={"$set": record},
                        upsert=True,
                    )
                )
            counts = (file_name, project_ids[record["ccgp-project-id"]])
            workflow_prog_ops.append(
                pymongo.operations.UpdateOne(
                    filter={"project_id": record["ccgp-project-id"]},
                    update={"$addToSet": {"Metadata recieved": counts}},
                    upsert=True,
                )
            )

            parsed_ops.append(
                pymongo.operations.UpdateOne(
                    filter={"file_name": file_name},
                    update={"$set": {"file_name": file_name}},
                    upsert=True,
                )
            )
            print("Done processing file: " + "'" + file_name + "'")
            Path(file_name).unlink()

    except Exception as e:  # Try to catch erros when processing a file.
        error_message = traceback.format_exc()
        print(f"Caught exception processing file: {file_name}:")
        print(error_message)
        parsed_metadatas.update_one(
            filter={"file_name": file_name},
            update={"$set": {"error": error_message}},
            upsert=True,
        )
    print(f"{len(metadata_ops)=}, {len(parsed_ops)=}, {len(workflow_prog_ops)=} ")
    try:
        collection.bulk_write(metadata_ops)
        parsed_metadatas.bulk_write(parsed_ops)
        ccgp_workflow_progress.bulk_write(workflow_prog_ops)
    except BulkWriteError as bwe:
        pprint(bwe.details)


def add_biosample_accessions(db_client: pymongo.MongoClient,) -> None:
    """Reads attributes.tsv from BioSample submission to add biosample accessions to samples in database"""
    db = db_client["ccgp_dev"]
    collection = db["sample_metadata"]
    parsed_metadatas = db["parsed_metadata_files"]
    ccgp_workflow_progress = db["workflow_progress"]

    already_read = [doc.get("file_name") for doc in parsed_metadatas.find({})]
    drive = CCGPDrive()
    to_process = [
        item
        for item in drive.list_files_from_folder("NCBI Biosample Attributes")
        if item["name"] not in already_read
    ]
    if not to_process:
        print("Nothing to be done.")
        return
    operations = []
    for file in to_process:
        file_name = file["name"]
        drive.download_files(file)
        print(file_name)
        df = pd.read_csv(file_name, sep="\t", header=0)

        for _, row in df.iterrows():
            record = row.to_dict()
            operations.append(
                pymongo.operations.UpdateOne(
                    filter={
                        "*sample_name": record["sample_name"]
                        .replace(" ", "_")
                        .replace(".", "_")
                    },
                    update={"$set": {"biosample_accession": record["accession"]}},
                    upsert=False,
                )
            )
            print(f"Added {record['accession']} to {record['sample_name']}")

    try:
        collection.bulk_write(operations)
    except pymongo.errors.BulkWriteError as bwe:
        pprint(bwe.details)

    for file in to_process:
        file = Path(file["name"])
        parsed_metadatas.update_one(
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
    metadata.add_argument(
        "-f",
        dest="force",
        required=False,
        action="store_true",
        help="Force rerun all sheets",
    )
    metadata.add_argument(
        dest="file", nargs="?", default=None, help="Run this specific file"
    )
    attributes = subparser.add_parser(
        "attributes", description="Update biosample accessions"
    )
    both = subparser.add_parser("both", description="Update both")

    args = parser.parse_args()

    if args.command == "metadata":
        if args.force:
            force = True

        else:
            force = False

        update_metadata(db, force, args.file)
    elif args.command == "attributes":
        add_biosample_accessions(db)
    elif args.command == "both":
        update_metadata(db, force, args.file)
        add_biosample_accessions(db)


if __name__ == "__main__":
    main()
