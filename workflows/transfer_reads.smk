import pymongo
import pandas as pd
import sys
sys.path.append(
    ".."
)
from db import get_mongo_client

client = get_mongo_client()
db = client['ccgp_dev']
collection = db['sample_metadata']

docs = collection.find({"ccgp-project-id": config['pid']})
files = []
for doc in docs:
    try:
        for file in doc['files']:
            for f in file:
                files.append(f)
    except KeyError:
        print(f"{doc['*sample_name']} did not have any files.")
            
rule all:
    input:
        expand("done_files/{project_id}/done_{file}", file=files, project_id=config['pid'])
        
rule download_from_s3:
    output:
        temp("ccgp/{file}")
    shell:
        "aws s3 cp s3://ccgp/{wildcards.file} ./ccgp/ --endpoint='http://10.50.1.41:7480/'"

rule upload_to_google:
    input:
        "ccgp/{file}"
    output:
        touch("done_files/{project_id}/done_{file}")
    shell:
        "gsutil cp -n {input} gs://ccgp-raw-reads/{config[pid]}/"

