import pandas as pd
def transform_dataset1(bronze_file):
    columns_to_keep = [
        'Year', 'LocationDesc', 'GeographicLevel', 'Topic',
        'Data_Value', 'Data_Value_Unit', 'Data_Value_Type',
        'StratificationCategory1', 'Stratification1', 'Y_lat', 'X_lon'
    ];
    df = pd.read_csv(bronze_file)
    df_filtered = df[columns_to_keep]
    df.to_csv(bronze_file, index=False)
def transform_dataset2():
    print('Hello World')

def transform_dataset3():
    print('Hello World')


