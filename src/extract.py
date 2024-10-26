# dataset is from: https://catalog.data.gov/dataset/heart-disease-mortality-data-among-us-adults-35-by-state-territory-and-county-2019-2021
import pandas as pd
import os
import shutil

def extract_raw_data(raw_folder_path, bronze_folder_path, log_file):
    processed_files = set()
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            processed_files = set(f.read().splitlines())
    if not os.path.isdir(raw_folder_path) or not os.path.isdir(bronze_folder_path):
        return {"status": "error", "message": "Invalid folder path(s)"}
    try:
        new_files = [file for file in os.listdir(raw_folder_path) if file.endswith('.csv') and file not in processed_files]
        for file in new_files:
            raw_file_path = os.path.join(raw_folder_path, file)
            bronze_file_path = os.path.join(bronze_folder_path, f'bronze_{file}')

            shutil.copy(raw_file_path, bronze_file_path)      

            with open(log_file, 'a') as f:
                f.write(f'{file}\n')
        if not new_files:
            print('no new files to process')
        else:
            print(new_files)
        return new_files
    except Exception as ex:
        print(f"Error extracting data from raw folder: {ex}")
        return
    
if __name__ == "__main__":
    extract_raw_data(
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/raw/',
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/bronze/',
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/processed_files.log') 