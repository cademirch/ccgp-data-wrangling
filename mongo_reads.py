import pymongo
import pandas as pd
import parse
from pathlib import Path
from pymongo.errors import BulkWriteError, CollectionInvalid
from pprint import pprint
import sys

client = pymongo.MongoClient("mongodb+srv://admin:layla123@cluster0.pe1we.mongodb.net/ccgp?retryWrites=true&w=majority")
db = client['ccgp']
collection = db['ccgp-samples']

sample_names = list(collection.find({}, {"*sample_name": 1}))

directory = Path(sys.argv[1])
fastq_files = list(directory.glob('*.fastq.gz'))

operations = []


if sample_names:
    for sample in sample_names:
        name = sample['*sample_name']
        found_files = [file for file in fastq_files if name in file.name]
        if found_files:
            found_files = [str(file.name) for file in found_files]
            operations.append(
                pymongo.operations.UpdateOne({'*sample_name': sample['*sample_name']}, {'$addToSet': {'fastq_files': found_files}})
            )


try:
    collection.bulk_write(operations)
except BulkWriteError as bwe:
    pprint(bwe.details)