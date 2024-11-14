import pandas as pd
def transform_to_silver_layer(bronze_file):
    bronze_df = pd.read_csv(bronze_file)
    # Apply transformations to the DataFrame
    silver_df = transform_data(bronze_df)


    return df_filtered

def transform_data(bronze_df):
    columns_to_keep = [
        'Year', 'LocationDesc', 'Topic',
        'Data_Value', 'Data_Value_Unit', 'Data_Value_Type',
        'Stratification1', 'Y_lat', 'X_lon'
    ]
    df_filtered = df[columns_to_keep]
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
    print(f"Transformed data saved to {bronze_file}")
    return df_filtered;



print(transform_dataset1('C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/bronze/bronze_Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County___2019-2021.csv'))