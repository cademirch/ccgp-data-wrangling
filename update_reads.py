import boto3
import pymongo
from dotenv import load_dotenv
from os import getenv
from db import get_mongo_client
from pymongo.errors import BulkWriteError
from pprint import pprint


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
    """Updates reads db with new (not seen before) reads"""
    db = db_client["ccgp"]
    collection = db["reads"]

    operations = []
    for file in files:
        operations.append(
            pymongo.operations.UpdateOne(  # type: ignore
                filter={"file_name": file.key},
                update={
                    "$set": {
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
    db = db_client["ccgp"]
    metadata = db["ccgp-samples"]
    reads = db["reads"]
    sample_names = list(
        metadata.find({}, {"*sample_name": 1})
    )  # Get all samples regardless if it has files b/c they might want new files
    orphan_reads = list(reads.find({"orphan": True}))
    metadata_ops = []
    reads_ops = []
    for sample in sample_names:
        name = sample["*sample_name"]
        found_files = [item for item in orphan_reads if f"{name}_" in item["file_name"]]
        if found_files:
            date_recieved = found_files[0][
                "mdate"
            ]  #  Take mdate of first file for simplicity's sake
            filesize_sum = sum([file["filesize"] for file in found_files])
            files = [file["file_name"] for file in found_files]
            # print(name, files, date_recieved, filesize_sum)
            metadata_ops.append(
                pymongo.operations.UpdateOne(
                    filter={"*sample_name": name},
                    update={
                        "$addToSet": {"files": files},
                        "$set": {
                            "recieved": date_recieved,
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
