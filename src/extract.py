# dataset is from: https://catalog.data.gov/dataset/heart-disease-mortality-data-among-us-adults-35-by-state-territory-and-county-2019-2021
import pandas as pd
import os
import shutil

def extract_raw_data(raw_folder_path, bronze_folder_path, log_file):
    processed_files = set()
    try:
        files = [file for file in os.listdir(raw_folder_path) if file.endswith('.csv')]
        for file in files:
            raw_file_path = os.path.join(raw_folder_path, file)
            bronze_file_path = os.path.join(bronze_folder_path', f'bronze_{file}')

            shutil.copy(raw_file_path, bronze_file_path)      
            processed_files.append(f'{raw_folder_path}/{file}')
        print(f'Simulated AutoLoader has noticed these files: {processed_files}')
        return processed_files
    except Exception as ex:
        print(f"Error extracting data from raw folder: {ex}")
        return
    
if __name__ == "__main__":
    extract_raw_data(
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/raw/',
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/bronze/',
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/processed_file.log'
        
        ) 