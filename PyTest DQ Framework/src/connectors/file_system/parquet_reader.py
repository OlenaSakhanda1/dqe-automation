import pandas as pd
import psycopg2
import os
import yaml

# ✅ Читаємо креденшали з ENV або config.yaml
pg_host = os.getenv('DB_HOST', 'postgres')
pg_port = os.getenv('DB_PORT', '5432')
pg_name = os.getenv('DB_NAME', 'mydatabase')
pg_user = os.getenv('POSTGRES_SECRET_USR')
pg_password = os.getenv('POSTGRES_SECRET_PSW')

if not pg_user or not pg_password:
    # Якщо немає ENV, читаємо з config.yaml (локальний запуск)
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.yaml')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        pg_user = config['postgres']['user']
        pg_password = config['postgres']['password']

# ✅ Шлях для паркетів
parquet_path = os.path.join(os.getcwd(), 'parquet_output')
os.makedirs(parquet_path, exist_ok=True)

def generate_parquet():
    # ✅ Підключення до PostgreSQL
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

if __name__ == "__main__":
    generate_parquet()
