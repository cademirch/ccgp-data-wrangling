import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
grandparent_dir = os.path.abspath(os.path.join(parent_dir, '..'))
sys.path.append(grandparent_dir)
from utils.db import get_mongo_client
import time

'''
This script updates all of the "filesize_sum" fields on mongo. 
'''

client = get_mongo_client()
db = client['ccgp_dev']
reads_collection = db['reads']

def get_file_size(file_list):
    total_file_size = 0
    for file_name in file_list:
        document = reads_collection.find_one({"file_name": file_name}, {"_id": 0, "filesize": 1})
        if document:
            filesize = document.get("filesize", 0)
            if isinstance(filesize, (int, float)):
                total_file_size += int(filesize)
    return total_file_size

def check_sample_filesize_sum(sample_collection):
    projection = {"_id": 0, "*sample_name": 1, "files": 1, "filesize_sum": 1, "ccgp-project-id": 1}
    cursor = sample_collection.find({}, projection)
    
    for document in cursor:
        sample_name = document.get("*sample_name", "")
        files = document.get("files", [])
        filesize_sum = document.get("filesize_sum", 0)
        project_id = document.get("ccgp-project-id", "")

        # if filesize_sum == 0 and len(files) > 1:
        #     file_sum = get_file_size(files)
        #     sample_collection.update_one({"*sample_name": sample_name}, {"$set": {"filesize_sum": file_sum}})
        #     print(f'Updated Sample: {sample_name}, Filesize: {file_sum}, Project: {project_id}')
        
        if filesize_sum and len(files) > 1:
            file_sum = get_file_size(files)
            sample_collection.update_one({"*sample_name": sample_name}, {"$set": {"filesize_sum": file_sum}})
            #print(f'Updated Sample: {sample_name}, Filesize: {file_sum}, Project: {project_id}')
        else:
            print(f'Conditions not met for {sample_name} --- {project_id}, Files: {len(files)}')
        
def main():
    sample_collection = db['sample_metadata']

    check_sample_filesize_sum(sample_collection)

if __name__ == "__main__":
    main()