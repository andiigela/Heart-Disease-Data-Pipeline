import pandas as pd
def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as ex:
        print(f"Error extracting data from raw folder: {ex}")
        return