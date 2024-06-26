import sys
import os
# sys.path.append("..")
current_dir = os.path.dirname(os.path.abspath(__file__))

parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
grandparent_dir = os.path.abspath(os.path.join(parent_dir, '..'))
sys.path.append(grandparent_dir)
from utils.db import get_mongo_client
import pandas as pd
import re

'''
This script updates the reads collection with the ccgp-project-id that the file is associated with. 
'''


client = get_mongo_client()
db = client['ccgp_dev']
sample_metadata_collection = db['sample_metadata']
reads_collection = db["reads"]

fields_to_get = {"ccgp-project-id": 1, "files": 1, "_id": 0}
# test_ccgp_project_id = '78-Hetaerina'
sample_metadata_cursor = sample_metadata_collection.find({}, fields_to_get)

# sample_metadata_cursor = sample_metadata_collection.find(
#     {"ccgp-project-id": test_ccgp_project_id},
#     fields_to_get
# )
# unmatched_files = []
matched_file_list = []
all_files = []
for entry in sample_metadata_cursor:
    files = entry.get('files', [])
    ccgp_project_id = entry.get('ccgp-project-id')

    matched_documents = reads_collection.find(
        {'file_name': {'$in': files}}
    )

    matched_files = set(doc['file_name'] for doc in matched_documents)
    for filename in matched_files:
        matched_file_list.append(filename)

    for file in files:
        all_files.append(file)

    # unmatched_files.extend(set(files) - matched_files)

    reads_collection.update_many(
        {'file_name': {'$in': files}},
        {'$set': {'ccgp-project-id': ccgp_project_id}}
    )

difference_list = list(set(all_files) - set(matched_file_list))
with open('unmatched_files.txt', 'w') as file:
    if len(difference_list) > 0:
        file.write('\n'.join(difference_list))
    else:
        file.write("All Files Matched!")

print(f'Number of files not matched: {len(difference_list)}')
print('')
print('Done!')
