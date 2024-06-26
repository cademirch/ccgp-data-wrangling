import boto3
import pymongo
from dotenv import load_dotenv
from os import getenv
from db import get_mongo_client



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



def main():

    s3_files = set([obj.key for obj in list_s3_bucket_objs()])
    file_to_check = 'Brach.txt'
    output_file = 'Brach_MISSING.txt'

    missing_files = []
    with open(file_to_check, 'r') as txt_file:
        for line in txt_file:
            line_clean = line.strip()
            if line_clean not in s3_files:
                missing_files.append(line_clean)
    
    if missing_files:
        with open(output_file, "w") as file:
            file.write("\n".join(missing_files))
        print(f"Files found in Mongo, but not found in AWS have been logged in {output_file}")
    else:
        print("All files in MongoDB exist in AWS S3.")

if __name__ == "__main__":
    main()