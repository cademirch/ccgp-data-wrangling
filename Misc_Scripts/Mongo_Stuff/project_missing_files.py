
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
grandparent_dir = os.path.abspath(os.path.join(parent_dir, '..'))
sys.path.append(grandparent_dir)
from utils.db import get_mongo_client

'''
if "Metadata Recieved" and "# Samples has reads" columns do not equal each other, then run this script for the project to see which
samples have an empty "files" array on MongoDB.
'''
client = get_mongo_client()
db = client['ccgp_dev']
collection = db['sample_metadata']

matching_documents = collection.find({"ccgp-project-id": "74-Chrysomela"})

for document in matching_documents:
    files_array = document.get("files", [])
    if not files_array:
        sample_name = document.get("*sample_name", "Sample name not found")
        print(sample_name)
