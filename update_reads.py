import boto3
import pymongo
from dotenv import load_dotenv
from os import getenv
from db import get_mongo_client
from pymongo.errors import BulkWriteError
from pprint import pprint
from collections import defaultdict
import pandas as pd
import re


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


def update_db_all(db_client: pymongo.MongoClient, files):
    """Updates db with list of files."""
    db = db_client["ccgp_dev"]
    collection = db["reads"]

    operations = []
    for file in files:
        operations.append(
            pymongo.operations.UpdateOne(  # type: ignore
                filter={"file_name": file.key},
                update={
                    "$setOnInsert": {
                        "orphan": True,
                        "filesize": file.size,
                        "mdate": file.last_modified,
                    }
                },  # Since upsert is true, we dont need to set file_name explicitly.
                upsert=True,
            )
        )
    try:
        collection.bulk_write(operations)
    except BulkWriteError as bwe:
        pprint(bwe.details)


def link_files_to_metadata(db_client: pymongo.MongoClient):
    """Links fastq files to sample names in samples db."""
    db = db_client["ccgp_dev"]
    metadata = db["sample_metadata"]
    reads = db["reads"]
    sample_names = list(
        metadata.find({}, {"*sample_name": 1, "Preferred Sequence ID": 1})
    )  # Get all samples regardless if it has files b/c they might want new files
    orphan_reads = list(reads.find({}))
    print(f"Found {len(orphan_reads)} orphan reads.")
    metadata_ops = []
    reads_ops = []
    matches = 0
    matched_files = 0
    for sample in sample_names:
        name = sample["*sample_name"]
        pref_id = sample.get("Preferred Sequence ID")
        # print(f"{name=}, {pref_id=}")
        if (
            pref_id is not None
            and not pd.isna(pref_id)
            and len(str(pref_id)) > len(name)
        ):

            name = str(pref_id).replace(".", "-").replace(" ", "_")
        found_files = [
            item
            for item in orphan_reads
            if f"{name}"
            in re.sub("_S\d+?_L\d+?_R\d_\d+?.fastq.gz", "", item["file_name"])
        ]
        if not found_files:
            if "-" in name:
                cand_name = name.replace("-", "_")
                found_files = [
                    item
                    for item in orphan_reads
                    if f"{cand_name}"
                    in re.sub("_S\d+?_L\d+?_R\d_\d+?.fastq.gz", "", item["file_name"])
                ]
            elif "_" in name:
                cand_name = name.replace("_", "-")
                found_files = [
                    item
                    for item in orphan_reads
                    if f"{cand_name}"
                    in re.sub("_S\d+?_L\d+?_R\d_\d+?.fastq.gz", "", item["file_name"])
                ]
        if found_files:
            matches += 1
            matched_files += len(found_files)
            dates = defaultdict(list)
            for f in found_files:
                dates[f["mdate"].date()].append(f["mdate"])
            dates = [dates[d][0] for d in dates.keys()]
            filesize_sum = sum([file["filesize"] for file in found_files])
            files = [file["file_name"] for file in found_files]
            print(f"Matched sample_name: '{name}' with {files}.")
            metadata_ops.append(
                pymongo.operations.UpdateOne(
                    filter={"*sample_name": name},
                    update={
                        "$set": {
                            "files": files,
                            "recieved": dates,
                            "filesize_sum": filesize_sum,
                        },
                    },
                )
            )
            for file in files:
                reads_ops.append(
                    pymongo.operations.UpdateOne(
                        filter={"file_name": file}, update={"$set": {"orphan": False}}
                    )
                )
    if not matched_files:
        print("Found no matches, exiting.")
        return
    print(f"Matched {matched_files} orphan files with {matches} samples")
    try:
        metadata.bulk_write(metadata_ops)
        reads.bulk_write(reads_ops)
    except BulkWriteError as bwe:
        print(bwe.details)


def main():
    files = list_s3_bucket_objs()
    db_client = get_mongo_client()
    update_db_all(db_client, files)
    link_files_to_metadata(db_client)


if __name__ == "__main__":
    main()
