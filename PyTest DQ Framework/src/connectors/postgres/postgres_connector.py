import psycopg2
import pandas as pd

class PostgresConnectorContextManager:
    def __init__(self, db_host: str, db_name: str, db_port: int, db_user: str, db_password: str):
        self.db_host = db_host
        self.db_name = db_name
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.conn = None

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
            return self
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.conn:
            self.conn.close()

    def get_data_sql(self, sql: str) -> pd.DataFrame:
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
                colnames = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return pd.DataFrame(rows, columns=colnames)
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {e}")
