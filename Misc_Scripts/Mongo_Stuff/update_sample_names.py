import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))

parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
grandparent_dir = os.path.abspath(os.path.join(parent_dir, '..'))
sys.path.append(grandparent_dir)
from utils.db import get_mongo_client
import pandas as pd
import re
import copy
import numpy as np

client = get_mongo_client()
db = client['ccgp_dev']
collection = db['sample_metadata']

csv_to_access = '63A-Quercus.csv'
df_csv = pd.read_csv(csv_to_access)

not_found_samples = []


for i, row in df_csv.iterrows():
    
    sample_name_new = row['*sample_name']
    sample_name_old = row['*sample_name_change']

    document = collection.find_one({'*sample_name': sample_name_old})

    if document:
            # Transfer existing minicore_seq_id to old_minicore_seq_id
            collection.update_one({'_id': document['_id']}, {'$set': {'old_sample_name': document['*sample_name']}})
            # Update the existing minicore_seq_id with the new value
            collection.update_one({'_id': document['_id']}, {'$set': {'*sample_name': sample_name_new}})
    else:
        not_found_samples.append(sample_name_old)
        print(f"Document with {sample_name_old} not found in MongoDB")

if not_found_samples:
    for sample in not_found_samples:
        print(sample)