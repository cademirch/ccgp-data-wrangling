"""This module handles parsing of the minicore sheets and biosample metadata sheets that are submitted to the me."""
import pandas as pd
from pathlib import Path

from pandas.core.frame import DataFrame
# Need to coerce the minicore sheets to the ncbi biosample format then can just concat them

# Read all minicore sheets
def read_minicore_sheets() -> pd.DataFrame:
    df_list = []
    for file in Path("../metadata_submissions/minicore").glob("*"):
        if file.suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(file)
            df.drop(index=df.index[[0,1]], axis=0, inplace=True) # Drop the first two rows (info and example)
            df.drop(df.columns[[0]], axis=1, inplace=True)  # Drop the first column (sample number)
            df.dropna(how="all", inplace=True)
            df_list.append(df)
            #file.rename(f"../metada_submissions/minicore/parsed/{file.name}")
    out_df = pd.concat(df_list)
    rename_dict = {"SampleID*": "*sample_name",
          "Genus species*": "*organism",
          "decimal latitude*": "lat",
          "decimal longitude*": "long",
          "sample collection date*": "*collection_date",
          "Locality Name": "geo_loc_name",
          }
    out_df.rename(columns=rename_dict, inplace=True)
    cols_to_keep = ["*sample_name", "*organism", "Preferred Sequence ID","subspecies","gDNA extraction method*","long","lat","sample collection date*","geo_loc_name","Locality Description"]
    out_df.drop(columns=out_df.columns.difference(cols_to_keep), inplace=True)
    return out_df

def read_biosample_sheets() -> pd.DataFrame:
    df_list = []
    for file in Path("../metadata_submissions/not_minicore").glob("*"):
        if file.suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(file, header=12)
            
        elif file.suffix in [".tsv"]:
            df = pd.read_csv(file, header=12, sep="\t")
        else:
            continue
        df.dropna(how="all", inplace=True)
        df_list.append(df)
        #file.rename(f"../metada_submissions/not_minicore/parsed/{file.name}")
    out_df = pd.concat(df_list)
    return out_df

def get_big_df() -> pd.DataFrame:
    df = pd.concat([read_minicore_sheets(), read_biosample_sheets()])
    df = df.loc[:,~df.columns.duplicated()]
    df['*sample_name'] = df['*sample_name'].astype(str)
    df.reset_index(inplace=True)
    return df

def get_summary_df() -> pd.DataFrame:
    df = get_big_df()
    df['short_organism'] = df['*organism'].str.split(" ").str[0]
    summary = df.groupby("short_organism", as_index=False).size()
    return summary

