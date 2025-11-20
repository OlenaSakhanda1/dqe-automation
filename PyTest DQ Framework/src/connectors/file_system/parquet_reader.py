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
        file_path = os.path.join(self.parquet_path, f"{table_name}.parquet")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Parquet file for {table_name} not found at {file_path}")
        return pd.read_parquet(file_path)

if __name__ == "__main__":
    generate_parquet()
