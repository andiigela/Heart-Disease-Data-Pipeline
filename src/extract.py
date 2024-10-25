# dataset is from: https://catalog.data.gov/dataset/heart-disease-mortality-data-among-us-adults-35-by-state-territory-and-county-2019-2021
import pandas as pd
import os

def extract_raw_data(raw_folder_path):
    processed_files = []
    files = [file for file in os.listdir(raw_folder_path) if file.endswith('.csv')]
    for file in files:
        raw_file_path = os.path.join(raw_folder_path, file)
        bronze_output_data = os.path.join('C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/bronze/', f'bronze_{file}')
        df_raw = pd.read_csv(raw_file_path)    
        # df_raw.to_csv(bronze_output_data, index=False)
        # print(bronze_output_data)
    # try:
    #     files = [file for file in os.listdir(raw_folder_path) if file.endswith('.csv')]
    #     for file in files:
    #         raw_file_path = os.path.join(raw_folder_path, file)
    #         df_raw = pd.read_csv(raw_file_path)

    #         bronze_output_data = os.path.join('C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/bronze/', f'bronze_{file}')
    #         df_raw.to_csv(bronze_output_data, index=False)        
    #         processed_files.append(f'{raw_folder_path}/{file}')
    #         print(f'Raw data from {file} successfully extracted and saved to {bronze_output_data}')
    #     print(processed_files)
    #     return processed_files
    # except Exception as ex:
    #     print(f"Error extracting data from raw folder: {ex}")
    #     return
    
if __name__ == "__main__":
    extract_raw_data('C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/raw/') 