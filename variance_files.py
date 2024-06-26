from utils.db import get_mongo_client
from pymongo import MongoClient
import pandas as pd
import itertools
import argparse
import math
import pygsheets
from pygsheets import Cell
import gspread
import os
import time

# UPDATED
def google_sheet():
    gc = pygsheets.authorize(service_file=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    sh = gc.open("WGS_METADATA_DB")
    #wks = sh.worksheet_by_title("variance")
    wks2 = sh.worksheet_by_title("genome_size")

    project_ids = wks2.get_col(1, include_tailing_empty=False)[1:]
    print(project_ids)

    # Get columns 1 (project_id), 9, and 10 from the Google Sheet
    data = wks2.get_values(start='A1', end='B158', returnas='cells', include_tailing_empty=True)
    data2 = wks2.get_values(start='F1', end='G158', returnas='cells', include_tailing_empty=True)
    # print('')
    # print(data2)
    # print('')
    project_genome_sizes_dict = {}
    other_genome_sizes = {}

    for row in data2:
        column_index = row[0].value
        column_genome = row[1].value
        
        if column_genome == '':
            column_genome = 'NA'
        
        project_genome_sizes_dict[column_index] = column_genome

    for row in data:
        column_index = row[0].value
        column_genome = row[1].value
        
        if column_genome == '':
            column_genome = 'NA'
        
        other_genome_sizes[column_index] = column_genome

    for key, value in project_genome_sizes_dict.items():
        back_up = other_genome_sizes[key]
        if value == "NA":
            project_genome_sizes_dict[key] = back_up

    #print(project_genome_sizes_dict)
    
    return project_ids, project_genome_sizes_dict



def get_filesize(project_ids, genome_dict):
    client = get_mongo_client()
    db = client["ccgp_dev"]
    collection = db["sample_metadata"]
    gc = pygsheets.authorize(service_file=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    sh = gc.open("WGS_METADATA_DB")
    #wks = sh.worksheet_by_title("variance")

    #proj_and_depth_dict = {}

    for project_id in project_ids:
        filter_criteria = {"ccgp-project-id": project_id}
        print('')
        print(project_id)

        data_collection_bin = {
            "_id": 0,
            "*sample_name": 1,
            "filesize_sum": 1,
        }

        num_documents = collection.count_documents(filter_criteria)
        print(f'{num_documents} samples accessed.')
        

        df = pd.DataFrame(collection.find(filter_criteria, data_collection_bin))


        sample_dict = {}
        
        for i, row in df.iterrows():
            #file_size = row["filesize_sum"]
            file_size = row.get("filesize_sum", 0)
            sample_name = row["*sample_name"]
            sample_dict[sample_name] = file_size

        #proj_genome_size = genome_dict[project_id]
        proj_genome_size = genome_dict.get(project_id, 'NA')

        print(f'Genome Size: {proj_genome_size}')

        samp_less_than_5 = 0
        depth_avg_sum = 0
        project_avg_counter = 0

        
        # with open(output_path, 'w') as file:
            # file.write("Sample Name\tExpected Reads\tExpected Depth\tProject Average\n")
        data_list = []
        for key, value in sample_dict.items():
            data_row = {}
            if value >= 0:
                #num_reads = round(value * 0.014343707705070851) # add scale factor back  * 0.01161157.  0.01919695403702928
                #num_reads = round(value * 0.01919695403702928)
                num_reads = round(value * 0.013534218984527578)
                #num_reads = round(value * 0.013640500123099957) 
                #print(num_reads)
                if proj_genome_size != 'NA':
                
                    depth_avg = round((num_reads * 150) / float(proj_genome_size), 3)
                    depth_avg_sum += depth_avg
                    project_avg_counter += 1


                    if depth_avg < 5:
                        samp_less_than_5 += 1
                else:
                    depth_avg = 'No Genome Size to Grab'
            else:
                num_reads = 'No filesize recorded.'
                depth_avg = 'NA'
            
            data_row['Sample Name'] = key
            data_row['Expected Reads'] = num_reads
            data_row['Expected Depth'] = depth_avg
            data_list.append(data_row)
            

        print(f'# of Samples less than 5x: {samp_less_than_5}')

        df_final = pd.DataFrame(data_list)
        
        if project_avg_counter != 0:
            depth_proj = pd.to_numeric(df_final['Expected Depth'], errors='coerce').mean()
            project_avg_depth = round(depth_proj, 3)
        else:
            project_avg_depth = 'NA'

        # depth_average = pd.to_numeric(df_final['Expected Depth'], errors='coerce').mean()
        # if pd.isna(depth_average):
        #     depth_average = 'NA'
        # else:
        #     depth_average = round(depth_average, 3)
        df_final.loc[0, "Project Depth"] = project_avg_depth

        #df_final['Project Average'] = project_avg_depth

        output_folder = "variance"
        os.makedirs(output_folder, exist_ok=True)  # make sure output folder exists.
        output_file = f"{project_id}_variance.txt"
        output_path = os.path.join(output_folder, output_file)
        #proj_and_depth_dict[project_id] = project_avg_depth

        # cell_list = wks.find(project_id)
        # if cell_list:
        #     cell = cell_list[0]  # Extract the cell from the list
        #     row_index = cell.row
        #     wks.update_value(f'H{row_index}', samp_less_than_5)
        #     # Rest of your code...
        # else:
        #     print(f"Cell for project_id {project_id} not found.")
        #     #print(f'{key}\t{num_reads}\t{depth_avg}')
        df_final.to_csv(output_path, sep='\t', index=False)
        time.sleep(1)

    
    print('Done!')

    
def main():

    project_ids, genome_dict = google_sheet()
    mongo_data = get_filesize(project_ids, genome_dict)


if __name__ == "__main__":
    main()