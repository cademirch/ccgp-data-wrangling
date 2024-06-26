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


"""
Checks Mongo "files" column for any length greater than some number. Outputs "suspicius" file count sample names to txt output.
"""
client = get_mongo_client()
db = client['ccgp_dev']
collection = db['sample_metadata']

exclude_ccgp_project_ids = ["60-Puma", "57-Orcuttia", "57-Tuctoria"]
# Output file
output_file_path = "suspicious_samples.txt"

# Iterate through MongoDB entries
with open(output_file_path, "w") as output_file:
    for entry in collection.find():
        # Check if "files" field array has 10 or more entries
        if "files" in entry and len(entry["files"]) >= 11 and "ccgp-project-id" in entry and entry["ccgp-project-id"] not in exclude_ccgp_project_ids:
            # Collect sample name and add it to the output file
            sample_name = entry.get("*sample_name", "N/A")
            ccgp_project = entry.get("ccgp-project-id", "N/A")
            output_file.write(f"Sample Name: {sample_name}\n")
            output_file.write(f"CCGP Project ID: {ccgp_project}\n")
            # Collect and print file names
            output_file.write("File Names:\n")
            for file_name in entry["files"]:
                output_file.write(f"  {file_name}\n")

            output_file.write("\n")
            output_file.write("\n")

print(f"Output written to {output_file_path}")