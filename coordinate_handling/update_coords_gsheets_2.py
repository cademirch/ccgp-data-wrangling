import os
import argparse
import pymongo
import pandas as pd
import gspread
import itertools
import math
import time

'''
Second script to run when Ryan sends updated coordinates.
Updates coordinate info on the "edit" tab WGS gsheet.
'''

def update_googlesheet(xlsx_path):

    columns_to_get = ["*sample_name", "ccgp-project-id", "lat", "long", "protected_coords", "exclude", "township", "range", "section"]

    script_directory = os.path.dirname(os.path.abspath(__file__))
    relative_path = os.path.join(script_directory, 'coordinate_sheets', xlsx_path)
    excel_df = pd.read_excel(relative_path, usecols=columns_to_get)


    # sa = gspread.service_account(filename="gspread_test.json")
    # gsheet_main_document = sa.open("coordinates")
    # coord_sheet = "coordinates"
    # get_coord_sheet = gsheet_main_document.worksheet(coord_sheet)

    sa = gspread.service_account(filename="WGS_DB_service_acct.json")
    gsheet_main_document = sa.open("WGS_METADATA_DB")
    coord_sheet = "edit"
    get_coord_sheet = gsheet_main_document.worksheet(coord_sheet)

    data = get_coord_sheet.get_all_values()
    google_df = pd.DataFrame(data[1:], columns=data[0]) 
    sample_not_in_gsheet = []


    for i, row in excel_df.iterrows():
        sample_name = str(row["*sample_name"])
        print(f'Looking for {sample_name}')
        matching_row = google_df.loc[google_df["*sample_name"] == sample_name]
        
        if not matching_row.empty:
            cell = get_coord_sheet.find(sample_name)#Find the cell with the matching sample_name in the Google Sheet
            row1 = cell.row
            protected_coords = str(row["protected_coords"]).upper()
            exclude = str(row["exclude"]).upper()
            #if not row["exclude"]:
            if exclude == "TRUE":
                time.sleep(8)
                get_coord_sheet.update(f"B{row1}", row["ccgp-project-id"])
                get_coord_sheet.update(f"C{row1}", row["lat"])
                get_coord_sheet.update(f"D{row1}", row["long"])
                get_coord_sheet.update(f"G{row1}", row["protected_coords"])
                get_coord_sheet.update(f"H{row1}", row["exclude"])
                print(f'{sample_name} exclude  = TRUE')

            elif exclude == "FALSE":
                print('here')
                time.sleep(8)
                # township_value = str(row["township"]) if pd.notna(row["township"]) else ''
                # range_value = str(row["range"]) if pd.notna(row["range"]) else ''
                # section_value = str(row["section"]) if pd.notna(row["section"]) else ''

                get_coord_sheet.update(f"B{row1}", row["ccgp-project-id"])
                get_coord_sheet.update(f"C{row1}", row["lat"])
                get_coord_sheet.update(f"D{row1}", row["long"])
                get_coord_sheet.update(f"G{row1}", row["protected_coords"])
                get_coord_sheet.update(f"H{row1}", row["exclude"])
                get_coord_sheet.update(f"I{row1}", row["township"])
                get_coord_sheet.update(f"J{row1}", row["range"])
                get_coord_sheet.update(f"K{row1}", row["section"])
                # get_coord_sheet.update(f"I{row1}", township_value)
                # get_coord_sheet.update(f"J{row1}", range_value)
                # get_coord_sheet.update(f"K{row1}", section_value)
                print(f'{sample_name} exclude = FALSE')
            else:
                time.sleep(5)
                get_coord_sheet.update(f"B{row1}", row["ccgp-project-id"])
                get_coord_sheet.update(f"C{row1}", row["lat"])
                get_coord_sheet.update(f"D{row1}", row["long"])
                print(f'{sample_name}')

            print(f"Successfully updated: {sample_name}")
            print('')
        else:
            sample_not_in_gsheet.append(sample_name)
    
    num_not_gsheet = len(sample_not_in_gsheet)
    print("Update Complete!")
    print('')
    if num_not_gsheet > 0:
        print('WARNING: The following samples were not found on the google sheet!')
        for item in sample_not_in_gsheet:
            print(item)
                

def main():
    parser = argparse.ArgumentParser(description="Extract metadata for a specific project (ccgp-project-id) and data type (plant, vertebrate, invertebrate)")  # Add file type later... aka tsv or sra.
    parser.add_argument("file_name", type=str, help="Specify the .xlsx file")  # Initializes 'project_name' Argument.
    args = parser.parse_args()

    update_googlesheet(args.file_name)



if __name__ == "__main__":
    main()