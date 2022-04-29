import pygsheets
import numpy as np
import pandas as pd
import parse
from db import get_mongo_client


def update_wgs_gsheet(db_client) -> None:
    db = db_client["ccgp"]
    collection = db["ccgp-samples"]
    df = pd.io.json.json_normalize(collection.find({}))
    gc = pygsheets.authorize(service_file="google_secret.json")
    sh = gc.open("WGS_METADATA_DB")
    wks = sh.worksheet_by_title("raw")
    wks.set_dataframe(df, (1, 1), fit=True)

    ### summarize
    wks = sh.worksheet_by_title("summary")
    summ = parse.get_summary_df(df)

    wks.set_dataframe(summ, (1, 1))


def main():
    db = get_mongo_client()
    update_wgs_gsheet(db)


if __name__ == "__main__":
    main()
