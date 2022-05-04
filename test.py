# from db import get_mongo_client
# import pymongo
# from gdrive import CCGPDrive
# from pathlib import Path

# client = get_mongo_client()

# db = client["ccgp"]
# collection = db["parsed_metadata_files"]

# already_read_metadata = [
#     doc.get("file_name") for doc in db["parsed_metadata_files"].find({})
# ]


# drive = CCGPDrive()
# l = drive.list_files_from_folder("Non-Minicore Submissions")

# need_to_do = [item for item in l if item["name"] not in already_read_metadata]
# print(need_to_do)

# need_to_do = [Path(item["name"]) for item in need_to_do]
# print(need_to_do)


def t(*files):
    for file in files:
        print(file)


x = [{"asd": 1}, {"asd": 2}]
y = {"asd": 3}

t(*x)
t(y)
