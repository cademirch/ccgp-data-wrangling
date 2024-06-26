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
import pygsheets
from pygsheets import Cell
import gspread
import numpy as np

client = get_mongo_client()
db = client['ccgp_dev']
collection = db['sample_metadata']


query = {"ccgp-project-id": "31-Phyllospadix"}
documents = collection.find(query)

for document in documents:
    if "geo_loc_name" in document:
        document["Locality Description"] = document.pop("geo_loc_name")
        #print(f"Updated Locality Description: {document['Locality Description']}")
        collection.update_one({"_id": document["_id"]}, {"$set": {"Locality Description": document["Locality Description"]}})
        #print(f"Updated document with _id: {document['_id']}")

print("All documents updated successfully.")

client.close()