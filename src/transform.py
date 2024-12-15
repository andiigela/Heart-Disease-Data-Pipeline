import pandas as pd
import os
import json
from pyspark.sql import SparkSession
def get_file_modification_time(filepath):
    return os.path.getmtime(filepath)
def transform_to_silver_layer(bronze_filename, timestamp_file="timestamp_metadata.json"):
    timestamp_path = f"C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/{timestamp_file}"
    bronze_filepath = f"C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/bronze/{bronze_filename}"
    silver_filepath = f"C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/cleaned/silver_{bronze_filename}"

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
    # save silver df to the cleaned folder
    silver_df.to_csv(silver_filepath, index=False)

    print(f"Transformed data saved to aggregated folder with name: silver_{bronze_filename}")

    # Update the timestamp data
    timestamp_data[bronze_filename] = current_mod_time
    with open(timestamp_path, "w") as f:
        json.dump(timestamp_data, f)

    aggregate_to_gold_layer(silver_df, f"silver_{bronze_filename}", "aggregated_timestamp.json")

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
    df_filtered['Data_Value_Unit'] = df_filtered['Data_Value_Unit'].replace('per 100,000 population', 100000).astype(int)
    df_filtered = df_filtered.loc[df_filtered['Stratification1'] != 'Overall']  # Gender
    df_filtered['Smoothing_3_Year_Avg_Rate'] = df_filtered['Data_Value_Type'].apply(
        lambda x: 'Spatially Smoothed' if 'Spatially Smoothed' in x else 'Not Spatially Smoothed'
    )
    df_filtered = df_filtered.drop("Data_Value_Type", axis=1)
    df_filtered = df_filtered.rename(columns={"Data_Value_Unit": "Data_Value_Unit_Per_Population", "Stratification1": "Gender"})
    return df_filtered;
def aggregate_to_gold_layer(silver_df, silver_filename, timestamp_file):
    """Aggregate data from Silver Layer and save results to Gold Layer."""
    timestamp_path = f"C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/{timestamp_file}"
    gold_folder = f"C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/aggregated/"
    os.makedirs(gold_folder, exist_ok=True)  # Ensure the Gold directory exists
    aggregated_filepath = f"{gold_folder}gold_{silver_filename}"

    # Load or initialize the timestamp metadata
    if os.path.exists(timestamp_path):
        with open(timestamp_path, "r") as f:
            timestamp_data = json.load(f)
    else:
        timestamp_data = {}

    # Skip aggregation if no changes are detected
    if os.path.exists(aggregated_filepath) and timestamp_data.get(f"gold_{silver_filename}") == get_file_modification_time(aggregated_filepath):
        print(f"No changes detected for gold_{silver_filename}. Aggregation skipped.")
        return None

    # Perform aggregations
    aggregated_df = silver_df.groupby(['Year', 'LocationDesc', 'Gender']).agg(
        Total_Data_Value=('Data_Value', 'sum'),
        Avg_Data_Value=('Data_Value', 'mean'),
        Max_Data_Value=('Data_Value', 'max'),
        Min_Data_Value=('Data_Value', 'min')
    ).reset_index()

    # Save aggregated results to the Gold Layer
    aggregated_df.to_csv(aggregated_filepath, index=False)
    print(f"Aggregated data saved to gold_files folder with name: gold_{silver_filename}")

    # Update the timestamp data
    timestamp_data[f"gold_{silver_filename}"] = get_file_modification_time(aggregated_filepath)
    with open(timestamp_path, "w") as f:
        json.dump(timestamp_data, f)

    return aggregated_df

transform_to_silver_layer('bronze_Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County___2019-2021.csv')