import pymongo
import pandas as pd
import parse
from pymongo.errors import BulkWriteError
from pprint import pprint
client = pymongo.MongoClient("mongodb+srv://admin:layla123@cluster0.pe1we.mongodb.net/ccgp?retryWrites=true&w=majority")

df = parse.get_big_df()

db = client['ccgp']
collection = db['ccgp-samples']


operations = []
for _, row in df.iterrows():
    record = row.to_dict()
    operations.append(
        pymongo.operations.UpdateOne(filter={'*sample_name': record['*sample_name']}, update={'$set': record}, upsert=True)
    )
try:
    collection.bulk_write(operations)
except BulkWriteError as bwe:
    pprint(bwe.details)


