import pygsheets
import numpy as np
import pandas as pd
import parse
from db import get_mongo_client
from os import environ

def update_wgs_gsheet(db_client) -> None:
    db = db_client["ccgp_dev"]
    collection = db["sample_metadata"]
    df = pd.io.json.json_normalize(collection.find({}))
    gc = pygsheets.authorize(service_file=environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    sh = gc.open("WGS_METADATA_DB")
    # wks = sh.worksheet_by_title("raw")
    # wks.set_dataframe(df, (1, 1), fit=True)

    ### summarize
    wks = sh.worksheet_by_title("summary")
    summ = parse.get_summary_df(df)
    print(summ["# Unxpected Species recieved"])
    wks.set_dataframe(summ, (1, 1))


def main():
    db = get_mongo_client()
    update_wgs_gsheet(db)


if __name__ == "__main__":
    main()
