import pandas as pd
import os
import json
def get_file_modification_time(filepath):
    return os.path.getmtime(filepath)
def transform_to_silver_layer(bronze_filename, timestamp_file="timestamp_metadata.json"):
    timestamp_path = f"C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/{timestamp_file}"
    bronze_filepath = f"C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/bronze/{bronze_filename}"
    silver_filepath = f"C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/aggregated/silver_{bronze_filename}"

    current_mod_time = get_file_modification_time(bronze_filepath)
    # Initialize timestamp data if the file doesn't exist
    if os.path.exists(timestamp_path):
        with open(timestamp_path, "r") as f:
            timestamp_data = json.load(f)
    else:
        timestamp_data = {}

    # Check if the file has been modified since the last transformation
    if timestamp_data.get(bronze_filename) == current_mod_time:
        print(f"No changes detected in {bronze_filename}. Transformation skipped.")
        return None  # Return None if no transformation is done

    bronze_df = pd.read_csv(bronze_filepath)
    # Apply transformations to the DataFrame
    silver_df = transform_data(bronze_df)
    # save silver df to the aggregated folder
    silver_df.to_csv(silver_filepath, index=False)
    print(f"Transformed data saved to aggregated folder with name: silver_{bronze_filename}")

    # Update the timestamp data
    timestamp_data[bronze_filename] = current_mod_time
    with open(timestamp_path, "w") as f:
        json.dump(timestamp_data, f)

    return silver_df;

def transform_data(bronze_df):
    columns_to_keep = [
        'Year', 'LocationDesc', 'Topic',
        'Data_Value', 'Data_Value_Unit', 'Data_Value_Type',
        'Stratification1', 'Y_lat', 'X_lon'
    ]
    df_filtered = bronze_df[columns_to_keep]
    df_filtered.loc[:, 'Data_Value'] = df_filtered['Data_Value'].fillna(df_filtered['Data_Value'].median())
    df_filtered = df_filtered.dropna()
    df_filtered = df_filtered.drop_duplicates()
    df_filtered['Data_Value_Unit'] = df_filtered['Data_Value_Unit'].replace('per 100,000 population', 100000)
    df_filtered = df_filtered.loc[df_filtered['Stratification1'] != 'Overall']  # Gender
    df_filtered['Smoothing_3_Year_Avg_Rate'] = df_filtered['Data_Value_Type'].apply(
        lambda x: 'Spatially Smoothed' if 'Spatially Smoothed' in x else 'Not Spatially Smoothed'
    )
    df_filtered = df_filtered.drop("Data_Value_Type", axis=1)
    df_filtered = df_filtered.rename(columns={"Data_Value_Unit": "Data_Value_Unit_Per_Population", "Stratification1": "Gender"})
    return df_filtered;

print(transform_to_silver_layer('bronze_Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County___2019-2021.csv'))