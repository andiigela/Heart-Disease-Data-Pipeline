import os
import pandas as pd
from sqlalchemy import create_engine


gold_folder = "C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/aggregated/"


DB_USERNAME = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "heart_disease_db"

# Create database connection using SQLAlchemy
engine = create_engine(f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def load_csv_to_postgresql():
#    Each CSV file will be stored in a table named after the file (without extension).
    for filename in os.listdir(gold_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(gold_folder, filename)
            df = pd.read_csv(file_path)
            table_name = f"final_{os.path.splitext(filename)[0].split('_')[5]}"
            try:
                df.to_sql(table_name, engine, if_exists="replace", index=False)
                print(f"Successfully loaded {filename} into the PostgreSQL table '{table_name}'.")
            except Exception as e:
                print(f"Error loading {filename}: {e}")

if __name__ == "__main__":
    load_csv_to_postgresql()