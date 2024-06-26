import os
import re

def extract_batch_number(folder_name):
    match = re.search(r'([\w-]+)_multiqc_data', folder_name, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        return None

def check_multiqc_file(batch_number):
    file_path = f"{batch_number}_multiqc_data/multiqc_general_stats.txt"
    return os.path.exists(file_path)

def collect_batch_numbers():

    os.chdir(os.path.join('downloads', 'qc'))
    cwd = os.getcwd()
    batch_numbers = []
    no_qc_data = []

    for folder_name in os.listdir(cwd):
        if os.path.isdir(os.path.join(cwd, folder_name)):
            batch_number = extract_batch_number(folder_name)
            if batch_number is not None:
                if check_multiqc_file(batch_number):
                    batch_numbers.append(batch_number)
                else:
                    no_qc_data.append(batch_number)

    return batch_numbers, no_qc_data

batch_numbers, no_qc_data = collect_batch_numbers()


print("Collected Batch Numbers:", batch_numbers)
print('')
print('Batches with no QC Data:', no_qc_data)

os.chdir(os.path.join('..', '..'))
missing_qcdata_dir = os.path.join(os.getcwd(), 'batches_missing_qcdata')
os.makedirs(missing_qcdata_dir, exist_ok=True)
file_path = os.path.join(missing_qcdata_dir, 'batch_log.txt')

with open(file_path, 'w') as file:
    file.write('Does "multiqc_general_stats.txt" Exist?' + '\n')
    file.write('' + '\n')
    for missing_batch in no_qc_data:
        file.write(f'Missing QC Data for: {missing_batch}' + '\n')
    file.write('' + '\n')
    for batch in batch_numbers:
        file.write(f'Got QC Data for: {batch}' + '\n')
