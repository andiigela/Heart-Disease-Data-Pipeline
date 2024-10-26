# dataset is from: https://catalog.data.gov/dataset/heart-disease-mortality-data-among-us-adults-35-by-state-territory-and-county-2019-2021
import pandas as pd
import os
import shutil


def load_log(log_file):
    processed_files = {}
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            for line in f:
                file, mod_time = line.strip().split(',')
                processed_files[file] = float(mod_time)
    return processed_files

def save_log(log_file, processed_files):
    """Save the processed files with their modification times to the log file."""
    with open(log_file, 'w') as f:
        for file, mod_time in processed_files.items():
            f.write(f"{file},{mod_time}\n")

def extract_raw_data(raw_folder_path, bronze_folder_path, log_file):
    processed_files = load_log(log_file)
    new_or_modified_files = []

    if not os.path.isdir(raw_folder_path) or not os.path.isdir(bronze_folder_path):
        return "Invalid folder path(s)"
    try:
        for file in os.listdir(raw_folder_path):
            if file.endswith('.csv'):
                raw_file_path = os.path.join(raw_folder_path, file)
                bronze_file_path = os.path.join(bronze_folder_path, f'bronze_{file}')
                
                current_mod_time = os.path.getmtime(raw_file_path) # Check if file is new or modified
                if file not in processed_files or processed_files[file] < current_mod_time:
                    shutil.copy(raw_file_path, bronze_file_path)     
                    print(f"Processed file: {file}")
                    processed_files[file] = current_mod_time
                    new_or_modified_files.append(file)

                save_log(log_file, processed_files)

        return new_or_modified_files
    except Exception as ex:
        print(f"Error extracting data from raw folder: {ex}")
        return
    
if __name__ == "__main__":
    extract_raw_data(
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/raw/',
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/bronze/',
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/processed_files.log') 