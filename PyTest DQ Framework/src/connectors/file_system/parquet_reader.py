import pandas as pd
import psycopg2
import os
import yaml

pg_host = os.getenv('DB_HOST', 'postgres')
pg_port = os.getenv('DB_PORT', '5432')
pg_name = os.getenv('DB_NAME', 'mydatabase')
pg_user = os.getenv('POSTGRES_SECRET_USR')
pg_password = os.getenv('POSTGRES_SECRET_PSW')

if not pg_user or not pg_password:
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.yaml')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        pg_user = config['postgres']['user']
        pg_password = config['postgres']['password']

parquet_path = os.path.join(os.getcwd(), 'parquet_output')
os.makedirs(parquet_path, exist_ok=True)

def generate_parquet():
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_name,
        user=pg_user,
        password=pg_password
    )

    tables = [
        "facilities",
        "patients",
        "src_generated_facilities",
        "src_generated_patients",
        "src_generated_visits",
        "visits"
    ]

    for table in tables:
        query = f"SELECT * FROM {table};"
        print(f"Processing table: {table}")
        df = pd.read_sql(query, conn)
        file_path = os.path.join(parquet_path, f"{table}.parquet")
        df.to_parquet(file_path)
        print(f"✅ Saved {table} to {file_path}")

    conn.close()
    print("✅ All tables have been converted to Parquet.")


class ParquetReader:
    def __init__(self, parquet_path=None):
        self.parquet_path = parquet_path or os.path.join(os.getcwd(), 'parquet_output')

    def read_table(self, table_name):
        folder_path = os.path.join(self.parquet_path, table_name)
        file_path = os.path.join(self.parquet_path, f"{table_name}.parquet")
        if os.path.exists(folder_path):
            print(f"Reading parquet partitioned folder: {folder_path}")
            return pd.read_parquet(folder_path)
        elif os.path.exists(file_path):
            print(f"Reading parquet file: {file_path}")
            return pd.read_parquet(file_path)
        else:
            raise FileNotFoundError(
                f"Parquet folder or file for {table_name} not found at {folder_path} or {file_path}"
            )

    def process(self, include_subfolders=False):
        all_data = []
        for root, _, files in os.walk(self.parquet_path):
            for file in files:
                if file.endswith(".parquet"):
                    file_path = os.path.join(root, file)
                    df = pd.read_parquet(file_path)
                    all_data.append(df)
            if not include_subfolders:
                break
        if not all_data:
            raise FileNotFoundError(f"No parquet files found in {self.parquet_path}")
        return pd.concat(all_data, ignore_index=True)

if __name__ == "__main__":
    generate_parquet()

