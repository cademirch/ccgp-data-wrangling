import boto3
import argparse
import pymongo
from dotenv import load_dotenv
from os import getenv
import os
from pathlib import Path



def get_aws_files():

    load_dotenv()
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=getenv("aws_access_key_id"),
        aws_secret_access_key=getenv("aws_secret_access_key"),
        endpoint_url=getenv("endpoint_url"),
    )
    bucket = s3.Bucket("ccgp")

    bucket_list = []
    for obj in bucket.objects.all():
        bucket_list.append(obj.key)


    return bucket_list


    # return bucket.objects.all()

def check_files_on_aws(aws_files, file_names, name_wildcard, run_wildcard):
 
    working_directory = os.getcwd()
    print(f'WD: {working_directory}')
    
    target = f'downloads/done_files/{name_wildcard}'
    os.chdir(target)
    new_directory = os.getcwd()
    print(new_directory)


    log_target = f'download_contents/{name_wildcard}'
    os.chdir(working_directory)
    os.makedirs(log_target, exist_ok=True)
    os.chdir(log_target)

    log_directory = os.getcwd()

    duplicates = []

    
    file_name_list = [string.split("/")[-1] for string in file_names]
    file_path_log = os.path.join(log_directory, f"{run_wildcard}-Files-Downloaded.txt")
    with open(file_path_log, "w") as file:
        for files in file_name_list:
            file.write(f"{files}\n")
    print('Downloaded Files Log Created!')

    for file_name in file_name_list:
        if file_name in aws_files:
            #print(f'{file_name}')
            duplicates.append(file_name)
        else:
            print(f'no: {file_name}')

    total_dup = len(duplicates)
    if len(duplicates) == 0:
        print("No Duplicate Files on AWS! Proceeding with Download.")
        file_path = os.path.join(new_directory, f"{run_wildcard}-cleared.txt")
        with open(file_path, "w") as file:
            pass
    else:
        file_path = os.path.join(new_directory, f"{run_wildcard}-Files-In-AWS.txt")
        print(f"WARNING! {total_dup} files already on AWS. Terminating Pipeline.")
        with open(file_path, "w") as file:
            for file_name in duplicates:
                file.write(f"{file_name}\n")




def main():

    input_data=set(snakemake.input)
    NAME = snakemake.params["name"]
    RUN = snakemake.params["run"]
    aws_files = get_aws_files()
    check_files_on_aws(aws_files, input_data, NAME, RUN)

if __name__ == "__main__":
    main()
