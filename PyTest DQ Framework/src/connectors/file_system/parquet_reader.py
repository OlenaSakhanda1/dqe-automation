import pandas as pd
import psycopg2
import os

# ✅ Читаємо креденшали з Jenkins environment
pg_host = os.getenv('DB_HOST', 'postgres')
pg_port = os.getenv('DB_PORT', '5432')
pg_name = os.getenv('DB_NAME', 'mydatabase')
pg_user = os.getenv('POSTGRES_SECRET_USR')
pg_password = os.getenv('POSTGRES_SECRET_PSW')

# ✅ Шлях для паркетів (створюємо у робочій директорії Jenkins)
parquet_path = os.path.join(os.getcwd(), 'parquet_output')
os.makedirs(parquet_path, exist_ok=True)

def get_connection():
    return psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_name,
        user=pg_user,
        password=pg_password
    )

# ✅ Список таблиць для конвертації
tables = [
    "facilities",
    "patients",
    "src_generated_facilities",
    "src_generated_patients",
    "src_generated_visits",
    "visits"
]

# ✅ Генеруємо Parquet для кожної таблиці
for table in tables:
    query = f"SELECT * FROM {table};"
    print(f"Processing table: {table}")
    df = pd.read_sql(query, conn)
    file_path = os.path.join(parquet_path, f"{table}.parquet")
    df.to_parquet(file_path)
    print(f"✅ Saved {table} to {file_path}")

conn.close()
print("✅ All tables have been converted to Parquet.")

