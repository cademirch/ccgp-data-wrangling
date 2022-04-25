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


def update_metadata(db_client: pymongo.MongoClient):
    """Parses metadata sheets from minicore and non minicore sources and updates database."""
    db = db_client["ccgp"]
    collection = db["ccgp-samples"]
    df = parse.get_big_df()
    operations = []
    for _, row in df.iterrows():
        record = row.to_dict()
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


def add_biosample_accessions(
    db_client: pymongo.MongoClient,
) -> None:
    """Reads attributes.tsv from BioSample submission to add biosample accessions to database"""
    db = db_client["ccgp"]
    collection = db["ccgp-samples"]
    ccgp_workflow_progress = db["ccgp_workflow_progress"]
    for tsv_file in Path("../biosample_attributes").glob("*.tsv"):
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
