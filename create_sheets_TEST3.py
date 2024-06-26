"""
CCGP MongoDB --> NCBI TSV Conversion Script

Purpose: This script was designed to grab MongoDB metadata for a CCGP Project of interest and generate a TSV file (biosample or sra) of the metadata. 

Date Created: July 24, 2023
Last Modified: August 4, 2023.

Arguments:
    ccgp-project-id (string): The name of the ccgp project of interest.
    data_type (string): The taxon group of the ccgp-project-id (types: 'plant', 'vertebrate', 'invertebrate')
    sheet_type (string): The desired TSV output file type (types: 'biosample', 'sra')

Example of Command Line:
    python3 create_sheets_TEST.py {project-id} {taxon} {sheet-type}
    python3 create_sheets_TEST.py 41-Thomomys vertebrate sra
    

Output Files:
    Generated output "sra" or "biosample" TSV sheets are sent to "sra_sheets_NEW" or "biosample_sheets_NEW", respectively. 

"""

import os
import argparse
import pymongo
import pandas as pd
import gspread
import traceback
import itertools
import numpy as np
import re
from thefuzz import process
import copy
from utils.db import get_mongo_client
from geopy.geocoders import Nominatim
from colorama import Fore, Style
import math

dim = "\033[2m"
def Connect_MongoDB():
    """
    Method connects to MongoDB client and returns the sample_metadata collection.
    """
    client = get_mongo_client()
    db = client["ccgp_dev"]
    collection = db["sample_metadata"]

    return collection


def Identify_Fields(collection):
    """
    Method collects all field's associated with the ccgp-project-id of interest.

    Returns:
        all_project_fields: Contains the dictionary keys of all fields of ccgp-project-id of interest.
    """
    sample_document = collection.find_one()
    if sample_document:
        all_project_fields = sample_document.keys()

    else:
        all_project_fields = []

    return all_project_fields


def Write_Metadata(collection, all_project_fields, project_name, data_type, sheet_type):
    """
    Method collects all metadata (fields and values) from MongoDB and organizes them based on data_type and sheet_type.
    Different fields are required for each data_type for 'biosample' sheet_type; Same fields used for all data_type's for 'sra' sheet_type.
    """
    query = {"ccgp-project-id": project_name}
    project_id = query["ccgp-project-id"]

    mongoDB_fields = all_project_fields
    data_collection_bin = {}

    if (sheet_type == "biosample"):  # Checks to see if sheet_type argument is biosample. If true, creates a biosample sheet csv.
        if data_type == "plant":  # Checks to see if data_type argument is plant.
            data_collection_bin = { # Define needed fields when taxon = PLANT.
                "_id": 0,
                "*sample_name": 1,  # REQUIRED
                "sample_title": 1,
                "bioproject_accession": 1,
                "*organism": 1,  # REQUIRED
                "isolate": 1,
                "cultivar": 1,
                "ecotype": 1,
                "age": 1,
                "dev_stage": 1,
                "collection_date": 1,  # REQUIRED
                "*geo_loc_name": 1,  # REQUIRED
                "County": 1,
                "State": 1,
                "tissue": 1,  # REQUIRED
                "biomaterial_provider": 1,
                "cell_line": 1,
                "cell_type": 1,
                "collected_by": 1,
                "culture_collection": 1,
                "disease_stage": 1,
                "genotype": 1,
                "growth_protocol": 1,
                "height_or_length": 1,
                "isolation_source": 1,
                "lat_lon": 1,
                "phenotype": 1,
                "population": 1,
                "sample_type": 1,
                "sex": 1,
                "specimen_voucher": 1,
                "temp": 1,
                "treatment": 1,
                "Locality Description": 1,
                "description": 1,
                "minicore_seq_id": 1,
                # "lat": 1,
                # "long": 1,
                # "protected_coords": 1,
                # "exclude": 1,
                # "township": 1,
                # "range": 1,
                # "section": 1

            }

        elif (data_type == "vertebrate"):  # Checks to see if data_type argument is vertebrate.
            data_collection_bin = { # Define needed fields when taxon = VERTEBRATE.
                "_id": 0,
                "*sample_name": 1,  # REQUIRED
                "sample_title": 1,
                "bioproject_accession": 1,
                "*organism": 1,  # REQUIRED
                "strain": 1,
                "isolate": 1,
                "breed": 1,
                "cultivar": 1,
                "ecotype": 1,
                "age": 1,
                "dev_stage": 1,
                "collection_date": 1,  # REQUIRED?
                "*geo_loc_name": 1,  # REQUIRED
                "County": 1,
                "State": 1,
                "sex": 1,  # REQUIRED
                "*tissue": 1,  # REQUIRED
                "biomaterial_provider": 1,
                "birth_date": 1,
                "birth_location": 1,
                "breeding_history": 1,
                "breeding_method": 1,
                "cell_line": 1,
                "cell_subtype": 1,
                "cell_type": 1,
                "collected_by": 1,
                "culture_collection": 1,
                "death_date": 1,
                "disease": 1,
                "disease_stage": 1,
                "genotype": 1,
                "growth_protocol": 1,
                "health_state": 1,
                "isolation_source": 1,
                "lat_lon": 1,
                "phenotype": 1,
                "sample_type": 1,
                "specimen_voucher": 1,
                "store_cond": 1,
                "stud_book_number": 1,
                "treatment": 1,
                "Locality Description": 1,
                "description": 1,
                "minicore_seq_id": 1,
            }

        elif (data_type == "invertebrate"):  # Checks to see if data_type argument is invertebrate.
            data_collection_bin = { # Define needed fields when taxon = INVERTEBRATE.
                "_id": 0,
                "*sample_name": 1,  # REQUIRED
                "sample_title": 1,
                "bioproject_accession": 1,
                "*organism": 1,  # REQUIRED
                "isolate": 1,
                "breed": 1,
                "host": 1,
                "isolation_source": 1,
                "collection_date": 1,  # REQUIRED
                "*geo_loc_name": 1,  # REQUIRED
                "County": 1,
                "State": 1,
                "tissue": 1,  # REQUIRED
                "age": 1,
                "altitude": 1,
                "biomaterial_provider": 1,
                "collected_by": 1,
                "depth": 1,
                "dev_stage": 1,
                "env_broad_scale": 1,
                "host_tissue_sampled": 1,
                "identified_by": 1,
                "lat_lon": 1,
                "sex": 1,
                "specimen_voucher": 1,
                "temp": 1,
                "Locality Description": 1,
                "description": 1,
                "minicore_seq_id": 1,
            }

        else:
            raise ValueError("Invalid data type input. Valid data type inputs include: plant, vertebrate, or invertebrate")

    elif sheet_type == "sra":

        if data_type in ["plant", "vertebrate", "invertebrate"]:

            data_collection_bin = { # Define needed fields for SRA sheet (PLANT, VERTEBRATE, OR INVERTEBRTE).
                "_id": 0,
                "bioproject_accession": 1,
                "*sample_name": 1,
                "library_ID": 1,
                "*organism": 1,
                "title": 1,
                "library_strategy": 1,
                "library_source": 1,
                "library_selection": 1,
                "library_layout": 1,
                "platform": 1,
                "instrument_model": 1,
                "library_prep_method": 1,
                "filetype": 1,
                "files": 1,
                "filename": 1,
                "filename2": 1,
                "filename3": 1,
                "filename4": 1,
            }
    else:
        raise ValueError("Invalid sheet type input. Valid sheet type inputs include: biosample, sra")
    
    num_documents = collection.count_documents(query)  # Fetches # of samples with metadata in ccgp-project-id of interest.
    print('')
    print(dim + Fore.YELLOW + '_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_' + Style.RESET_ALL)
    print(dim + Fore.BLUE+'         ~ MetaData Information ~'+ Style.RESET_ALL)
    print('')
    print(f"Number of documents fetched for {project_name} ({data_type}, {sheet_type}): {Fore.RED + str(num_documents)+ Style.RESET_ALL} samples fetched from MongoDB.")  # debug test
    print(dim + Fore.YELLOW + '-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-' + Style.RESET_ALL)
    print('')
    
    return collection.find(query, data_collection_bin), data_collection_bin, project_id


def generate_TSV(get_project_metadata, data_collection_bin, all_project_fields, project_name, sheet_type, collection):
    """
    Method fetches data from MongoDB for both biosample and sra, biosample sample_names sent to gsheets_TEST.py to get coordinate info. SRA sheet generation happens here.
    """

    df = pd.DataFrame(get_project_metadata)  # Creates a data frame.
    df = df.reindex(columns=data_collection_bin.keys())  # Reorder the columns based on the desired order from data_collection_bin.
    required_columns = ["*sample_name", "*organism", "collection_date", "*geo_loc_name", "*tissue", "age", "dev_stage"] # NCBI required columns. Add more or less depeneding on what NCBI wants. If field is in this list, missing/NaN values will be added.
    required_sra_cols = ["design_description"]

    sample_names_to_compare = []

    if sheet_type == "biosample":  # Handles filling out necessary columns with data and TSV generation for the 'biosample' sheet_type.

        for i, row in df.iterrows(): # Loop through each row of the Pandas DataFrame.
            if (pd.isnull(row["*geo_loc_name"]) or row["*geo_loc_name"] == "") and (pd.isnull(row["Locality Description"]) or row["Locality Description"] == ""):
                #df.at[i, "*geo_loc_name"] = 'not provided'
                county_name = df.loc[i, "County"]
                state_name = df.loc[i, "State"]
                if (pd.isnull(row["County"]) or row["County"] == "") and (pd.isnull(row["State"]) or row["State"] == ""):
                    df.at[i, "*geo_loc_name"] = "missing"
                elif (pd.isnull(row["County"]) or row["County"] == "") and (row["County"]):
                     df.at[i, "*geo_loc_name"] = "USA: " + str(state_name) 
                elif (pd.isnull(row["State"]) or row["State"] == "") and (row["County"]): # NOT SURE ABOUT THIS.
                    df.at[i, "*geo_loc_name"] = "missing"
                elif (row["County"]) and (row["County"]):
                    df.at[i, "*geo_loc_name"] = "USA: " + str(state_name) + ": " + str(county_name)

            
            elif (row["*geo_loc_name"]) and (pd.isnull(row["Locality Description"]) or row["Locality Description"] == ""):
                df.at[i, "*geo_loc_name"] = df.loc[i, "*geo_loc_name"]
            
            elif (pd.isnull(row["*geo_loc_name"]) or row["*geo_loc_name"] == "") and (row["Locality Description"]):
                df.at[i, "*geo_loc_name"] = df.loc[i, "Locality Description"]
            
            elif row["*geo_loc_name"] and row["Locality Description"]:
                geo_name = df.loc[i, "*geo_loc_name"]
                loc_description = df.loc[i, "Locality Description"]
                df.at[i, "*geo_loc_name"] = geo_name + ":" + loc_description

            if pd.isnull(row["sample_title"]) or row["sample_title"] == "":
                df.at[i, "sample_title"] = row["minicore_seq_id"]

        for col in required_columns: # Loop through each required folumn and replace blanks with "missing".
            if col not in df.columns:
                df[col] = ""

            df[col].replace("", "not provided", inplace=True) # Replace blanks with "missing".
            df[col].fillna("not provided", inplace=True) # Columns that MUST contain controlled vocab are accounted for here. Blanks (NaN) are replaced with string "missing".

        org = df["*organism"].str.replace(" ", "_")
        df["isolate"] = org + "_" + df["*sample_name"] # Add data to BIOSAMPLE column "isolate".
        
        df.drop("Locality Description", axis = 1, inplace = True)
        if "County" in df.columns:
            df.drop("County", axis=1, inplace=True)
        else:
            print("Column 'County' not found in DataFrame.")
        #df.drop("State", axis = 1, inplace = True)
        if "State" in df.columns:
            df.drop("State", axis=1, inplace=True)
        else:
            print("State 'State' not found in DataFrame.")
        sample_names_to_compare.extend(df["*sample_name"].tolist())

        # After processing and updating the DataFrame as needed, remove the 'minicore_seq_id' column
        if 'minicore_seq_id' in df.columns:
            df.drop('minicore_seq_id', axis=1, inplace=True)


        return df, sample_names_to_compare  # returns data frames

    elif sheet_type == "sra":  # Handles filling out necessary columns with data and TSV generation for the 'sra' sheet_type.

        client_reads = get_mongo_client()
        db_r = client_reads["ccgp_dev"]
        collection_reads = db_r["reads"]

        for i, row in df.iterrows(): # Loop through each row of the Pandas DataFrame.
            if (pd.isnull(row["library_prep_method"]) or row["library_prep_method"] == ""):
                df.at[i, "library_prep_method"] = "missing"

        df.rename(columns={"library_prep_method": "design_description"}, inplace=True)

        defaults = {  # Default values associated with column (key) titles.
            "title": "Whole genome sequencing of " + df["*organism"],
            "library_strategy": "WGS",
            "library_source": "GENOMIC",
            "library_selection": "RANDOM",
            "library_layout": "PAIRED",
            "platform": "Illumina",
            "instrument_model": "Illumina NovaSeq 6000",
            "filetype": "fastq"
        }
        
        for field, default_value in defaults.items(): # Add default metadata (values) to all columns (keys) from dictionary defaults.
            if field in df.columns:
                df[field].fillna(default_value, inplace=True)

            else:
                df[field] = default_value

        highest_len_files = 0 # Counter to maintain the most amount of files representing a single sample from the project.
        list_of_dicts = []
        i = 0
        match_count = 0
        for _, row in df.iterrows():
            record = row.to_dict()
            try:
                files = sorted(record["files"])
                #files = record["files"]  # MESS WITH THIS IF FILE _R1_ AND _R2_ ARE OUT OF ORDER!
                i += 1

                if len(files) % 2 == 0:
                    for j in range(0, len(files), 2):

                        # print('')
                        # print(f'FILES: {files}')
                        # print(f'j = {j}')
                        
                        # Create a new record for each pair of files
                        record_copy = copy.deepcopy(record)
                        record_copy["filename"] = files[j]
                        #print(record_copy["filename"])
                        record_copy["filename2"] = files[j + 1]
                        #print(record_copy["filename2"])
                        #print('')
                        record_copy["library_ID"] = files[j].split("_R1")[0]
                        record_copy["title"] = f"Whole genome sequencing of {record_copy['*organism']}"

                        document_test = collection_reads.find_one({"file_name": files[j]})
                        # print(document_test)
                        # print('')
                        if document_test and "instrument_model" in document_test:
                # Replace the value in your DataFrame with the default instrument value
                            record_copy['instrument_model'] = document_test["instrument_model"]

                        list_of_dicts.append(record_copy)
                        len_files = len(files)

                        if len_files > highest_len_files:
                            highest_len_files = len_files

            except TypeError:
                continue
        
        out_df = pd.DataFrame(list_of_dicts)
        print('')
        print(f'# of Files = {len_files}')
        print(f'HIGHEST # of Lanes = {match_count}')
        print('')

        df.drop("*organism", axis = 1, inplace = True) # Delete *organism column from sra sheet after title column is filled to completion.
        out_df.drop("files", axis = 1, inplace = True)
        if match_count == 2:
            out_df.drop("filename3", axis = 1, inplace = True)
            out_df.drop("filename4", axis = 1, inplace = True)
        if match_count == 3:
            out_df.drop("filename4", axis = 1, inplace = True)
        df.rename(columns={"*sample_name": "sample_name"}, inplace=True)
        output_folder = "sra_sheets_NEW"
        os.makedirs(output_folder, exist_ok=True)  # make sure output folder exists.
        output_file = f"{project_name}_sra.tsv"
        output_path = os.path.join(output_folder, output_file)
        out_df.to_csv(output_path, sep="\t", index=False)

    print(f"Number of rows in DataFrame for {project_name} ({sheet_type}): {len(df)}")  # Debug print statement. DataFrames should = # of samples.


def create_biosample_TSV(get_project_metadata, data_collection_bin, all_project_fields, project_name, sheet_type, df_data, all_proj_data_bin, coord_info, grab_loc_data):
    
    if sheet_type == "biosample":  # Handles filling out necessary columns with data and TSV generation for the 'biosample' sheet_type.
        
        for sample_name, info in coord_info.items():
            protected_coords_check = info["protected_coords_check"]
            exclude_check = info["exclude_check"]
            lat_value = info["lat_value"]
            long_value = info["long_value"]
            township_value = info["township"]
            range_value = info["range"]
            section_value = info["section"]

            protected_coords_check = str(protected_coords_check).upper() if not pd.isna(protected_coords_check) else ''
            exclude_check = str(exclude_check).upper() if not pd.isna(exclude_check) else ''
            # if not math.isnan(protected_coords_check) and not math.isnan(exclude_check):
            #     protected_coords_check = str(protected_coords_check).upper()
            #     exclude_check = str(exclude_check).upper()
            # else:
            #     protected_coords_check = ''
            #     exclude_check = ''

            if protected_coords_check == "TRUE" and exclude_check == "TRUE":
                all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, "lat_lon"] = "not provided"

            elif protected_coords_check == "TRUE" and exclude_check == "FALSE":
                cali = 'USA:California,'
           
                geo_loc = str(grab_loc_data[sample_name]) + ', ' + str(township_value) + '-' + str(range_value) + '-' + str(section_value) #output = USA:[state], township-range-section
                # print(geo_loc)
                # geo_loc = str(township_value) + '-' + str(range_value) + '-' + str(section_value) #output = township-range-section
                # print(geo_loc)
                # print('')

                existing_geo_loc = all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, "*geo_loc_name"].values[0] # maybe delete this if glitch.
                #print(f'1: {existing_geo_loc}')
                if existing_geo_loc and geo_loc != "not provided": # maybe delete this if glitch.
                    print("existing geo loc is true")
                    all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, ["*geo_loc_name"]] = geo_loc # maybe delete this if glitch.
                else: # maybe delete this if glitch.
                    all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, ["*geo_loc_name"]] = geo_loc # maybe delete this if glitch.
                print('')

            elif (protected_coords_check == "FALSE" and exclude_check == "FALSE") or (protected_coords_check == "NAN" and exclude_check == "NAN") or (not protected_coords_check and not exclude_check):
            # rest of your code

                #print(f' {lat_value}, {long_value}')
                if lat_value == "NaN" and long_value == "NaN":
                    lat_long_string = "not provided"
                else:
                    lat_long_string = str(lat_value) + ',' + str(long_value)

                geo_loc = str(grab_loc_data[sample_name]) # This grabs the USA:California stuff.
                #print(geo_loc) 
                
                
                existing_geo_loc = all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, "*geo_loc_name"].values[0]
                #print(f'2: {existing_geo_loc}')
                if existing_geo_loc and geo_loc != "not provided":
                    if existing_geo_loc == "missing":
                        all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, ["lat_lon"]] = lat_long_string
                        all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, ["*geo_loc_name"]] = geo_loc
                    else:
                        all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, ["lat_lon"]] = lat_long_string
                        all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, ["*geo_loc_name"]] = geo_loc + ', ' + existing_geo_loc # This is USA:California, Locality Description
                        #all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, ["*geo_loc_name"]] = existing_geo_loc # This is just Locality Description.
                #geo_str = all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, 
                #geo_loc = str(grab_loc_data[sample_name])
                else:
                    all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, ["lat_lon"]] = lat_long_string
                    all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, ["*geo_loc_name"]] = geo_loc
        
        output_folder = "biosample_sheets_NEW"
        os.makedirs(output_folder, exist_ok=True)  # make sure output folder exists.
        output_file = f"{project_name}_biosample.tsv"
        output_path = os.path.join(output_folder, output_file)
        df_data.to_csv(output_path, sep="\t", index=False)


def google_sheets(df_data, sample_names_to_compare, sheet_type, project_id):
    proj_sampleNames_bin = sample_names_to_compare
    all_proj_data_bin = df_data
    coord_info = {}


    if sheet_type == "biosample":

        # sa = gspread.service_account(filename="coordinate_handling/gspread_test.json")
        # gsheet_main_document = sa.open("coordinates")  # The Name of the Google Sheet to be accessed.
        # coord_sheet = "coordinates"  # Name of the coordinate sheet document.
        # get_coord_sheet = gsheet_main_document.worksheet(coord_sheet)


        # data = get_coord_sheet.get_all_values()
        # google_df = pd.DataFrame(data[1:], columns=data[0]) 

        client = get_mongo_client()
        db = client['ccgp_dev']
        collection = db['sample_metadata']


        query = {"ccgp-project-id": project_id}
        data_collection_bin = {"_id": 0, "*sample_name": 1, "lat": 1, "long": 1, "protected_coords": 1, "exclude": 1, "township": 1, "range": 1, "section": 1}
        cursor = collection.find(query, data_collection_bin)

        coord_data_list = list(cursor)
        #print(coord_data_list)

        fields_to_select = [
            "*sample_name", 
            "lat", 
            "long", 
            "protected_coords",  # Default value if not present
            "exclude",  # Default value if not present
            "township",
            "range",
            "section"
        ]

        # Create a list of dictionaries with selected fields and default values
        # selected_data_list = [{field: doc.get(field, None) for field in fields_to_select} for doc in coord_data_list]
        # coord_df = pd.DataFrame(selected_data_list)

        coord_df = pd.DataFrame(coord_data_list)
        column_SampleName = coord_df["*sample_name"]

        protected_coord_count = 0
        unprotected_coord_count = 0
        exclude_count = 0
        hidden_message = "Precise location made hidden for sensitive species."
        lat_long_dict = {}
        for i, value in enumerate(column_SampleName):

            if value in proj_sampleNames_bin:
                relevant_row = coord_df[coord_df["*sample_name"] == value].iloc[0]
                protected_coords_check = relevant_row.get("protected_coords", "FALSE")
                exclude_check = relevant_row.get("exclude", "FALSE")

                township_value = ''
                range_value = ''
                section_value = ''
                lat_value = ''
                long_value = ''

                sample_name_check = relevant_row["*sample_name"]
                lat_value_check = relevant_row["lat"]
                long_value_check = relevant_row["long"]
                lat_long_dict[sample_name_check] = (lat_value_check, long_value_check)

                if protected_coords_check == "TRUE" and exclude_check == "TRUE":
                    sample_name = relevant_row["*sample_name"]
                    all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, "lat_lon"] = ""
                    all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, "description"] = hidden_message
                    protected_coord_count += 1
                    exclude_count += 1
                    

                elif protected_coords_check == "TRUE" and exclude_check == "FALSE":
                    sample_name = relevant_row["*sample_name"]
                    township_value = relevant_row["township"]
                    range_value = relevant_row["range"]
                    section_value = relevant_row["section"]
                    all_proj_data_bin.loc[all_proj_data_bin["*sample_name"] == sample_name, "description"] = hidden_message
                    protected_coord_count += 1

                #elif protected_coords_check == "FALSE" and exclude_check == "FALSE":
                else:
                    sample_name = relevant_row["*sample_name"]
                    lat_value = relevant_row["lat"]
                    long_value = relevant_row["long"]
                    #lat_long_dict[sample_name] = (lat_value, long_value)
                    unprotected_coord_count += 1
                


                coord_info[value] = {
                    "protected_coords_check": protected_coords_check,
                    "exclude_check": exclude_check,
                    "lat_value": lat_value,
                    "long_value": long_value,
                    "township": township_value,
                    "range": range_value,
                    "section": section_value
                }
    print('')
    print(dim + Fore.YELLOW + '_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_' + Style.RESET_ALL)
    print(dim + Fore.BLUE+'         ~ Coordinate Information ~'+ Style.RESET_ALL)
    print('')
    print(f' Protected Coord Count: {Fore.RED + str(protected_coord_count) + Style.RESET_ALL}')
    print(f' Exclude (Township & Range) Count: {Fore.RED + str(exclude_count) + Style.RESET_ALL}')
    print(f' UnProtected Coord Count: {Fore.RED + str(unprotected_coord_count) + Style.RESET_ALL}')
    print(dim + Fore.YELLOW + '-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-' + Style.RESET_ALL)
    print('')
    
   # print(coord_info) # lat and long here

    return all_proj_data_bin, coord_info, lat_long_dict

def Check_coord_location(lat_long_dict, df_data):
    
    geolocator = Nominatim(user_agent="CCGP Data Wrangling Script", timeout=10)
    non_state_count = 0
    total_sample_count = 0
    grab_loc_data = {}

    usa_states = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware",
    "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming"
    ]

    usa_states_dict = {state: 0 for state in usa_states}

# Simulate counting occurrences in an iteration

    print('')
    print(dim + Fore.YELLOW + '_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_' + Style.RESET_ALL)
    print(dim + Fore.BLUE+'         ~ Geo-Location Information ~' + Style.RESET_ALL)
    print('')
    print('InValid State Log:')
    for i, row in df_data.iterrows():
        if row["*geo_loc_name"]:
            geog_name = df_data.loc[i, "*geo_loc_name"]



    for key, (lat, long) in lat_long_dict.items():

        try:
            lat = float(lat)
            long = float(long)
        except ValueError:
            grab_loc_data[key] = 'not provided'
            print(f'Invalid coordinates for {key}')
            continue
        if math.isnan(lat) or math.isnan(long):
            grab_loc_data[key] = 'not provided'
            print(f'Invalid coordinates for {key}')
            continue

        location = geolocator.reverse((lat, long), exactly_one=True)

        if location:
            address = location.raw.get('address', {})
            state = address.get('state', 'Unknown')
            country = address.get('country', 'Unknown')
            if country == "United States":
                country = "USA"

            if state in usa_states_dict:
                usa_states_dict[state] += 1
                total_sample_count += 1
                # state_to_append = state
                # country_to_append = country
                geo_string = f'{country}:{state}'
                grab_loc_data[key] = geo_string

                #print(f'For {key}, the state is: {state}')
            else:
                non_state_count += 1
                total_sample_count += 1
                geo_string = f'{country}:{state}'
                grab_loc_data[key] = geo_string
                print(f'    Sample: ({Fore.MAGENTA + key + Style.RESET_ALL}) was NOT taken from a valid US state ({dim + Fore.MAGENTA + state + Style.RESET_ALL})')
            
        else:
            grab_loc_data[key] = 'USA:California,'
            print(f'Location data not found for {key}')

    print('')
    print('STATE COUNTS:')
    for state, count in usa_states_dict.items():
        if count > 0:
            print(f'    {state}: {Fore.RED+ str(count) + Style.RESET_ALL}')
        
    if non_state_count > 0:
        print(f'    Non-State(s): {Fore.RED + str(non_state_count) + Style.RESET_ALL}')
        print('')
        print(f'Total Sample Locations Accounted For: {Style.BRIGHT + Fore.RED + str(total_sample_count) + Style.RESET_ALL}')
        print(dim + Fore.YELLOW + '-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-' + Style.RESET_ALL)
    else:
        print(f'Total Sample Locations Accounted For: {Style.BRIGHT + Fore.RED + str(total_sample_count) + Style.RESET_ALL}')
        print(dim + Fore.YELLOW + Fore.YELLOW + '-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-'+ Style.RESET_ALL)


    return grab_loc_data


def main():
    try:
        parser = argparse.ArgumentParser(description="Extract metadata for a specific project (ccgp-project-id) and data type (plant, vertebrate, invertebrate)")  # Add file type later... aka tsv or sra.
        parser.add_argument("project_name", type=str, help="Specify the project name (associated with field 'ccgp-project-id')")  # Initializes 'project_name' Argument.
        parser.add_argument("data_type", choices=["plant", "vertebrate", "invertebrate"], help="Specify the taxon type (plant, vertebrate, invertebrate)")  # Initializes 'data_type' Argument.
        parser.add_argument("sheet_type", choices=["sra", "biosample"], help="Specify the output tsv file needed (sra or biosample)")  # Initializes 'sheet_type' Argument.
        args = parser.parse_args()

        collection = Connect_MongoDB()

        all_project_fields = Identify_Fields(collection)

        get_project_metadata, data_collection_bin, project_id = Write_Metadata(collection, all_project_fields, args.project_name, args.data_type, args.sheet_type)

        if args.sheet_type == "biosample":
            df_data, sample_names_to_compare = generate_TSV(get_project_metadata, data_collection_bin, all_project_fields, args.project_name, args.sheet_type, collection)

            all_proj_data_bin, coord_info, lat_long_dict = google_sheets(df_data, sample_names_to_compare, args.sheet_type, project_id)
            
            grab_loc_data = Check_coord_location(lat_long_dict, df_data)

            create_biosample_TSV(get_project_metadata, data_collection_bin, all_project_fields, args.project_name, args.sheet_type, df_data, all_proj_data_bin, coord_info, grab_loc_data)

            print("")
            print(Fore.LIGHTGREEN_EX + f"Success! The requested SHEET: Biosample, for PROJECT-ID: {args.project_name} was generated." + Style.RESET_ALL)
            print("")

        elif args.sheet_type == "sra":
            generate_TSV(get_project_metadata, data_collection_bin, all_project_fields, args.project_name, args.sheet_type, collection)
            print("")
            print(Fore.LIGHTGREEN_EX +f"Success! The requested SHEET: SRA, for PROJECT-ID: {args.project_name} was generated."+ Style.RESET_ALL)
            print("")

    except Exception as e:
        print("")
        print(Fore.RED+"An error has occured:", str(e) + Style.RESET_ALL)
        print("")
        print(Fore.RED+"Info:"+ Style.RESET_ALL)
        traceback.print_exc()
        print("")
        print('If GEOPY error --> Try changing the "timeout=" to a higher number')

if __name__ == "__main__":
    main()