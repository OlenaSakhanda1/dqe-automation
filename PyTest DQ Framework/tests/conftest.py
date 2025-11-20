import pytest
import os
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.connectors.file_system.parquet_reader import ParquetReader
from src.data_quality.data_quality_validation_library import DataQualityLibrary

def pytest_addoption(parser):
    parser.addoption("--db_host", action="store", default="localhost")
    parser.addoption("--db_port", action="store", default="5432")
    parser.addoption("--db_name", action="store", default="mydatabase")
    parser.addoption("--db_user", action="store")
    parser.addoption("--db_password", action="store")

@pytest.fixture(scope="session")
def db_credentials(request):
    user = request.config.getoption("--db_user") or os.getenv("POSTGRES_SECRET_USR")
    password = request.config.getoption("--db_password") or os.getenv("POSTGRES_SECRET_PSW")

    if not user or not password:
        pytest.skip("Database credentials are missing. Skipping DB tests.")

    return {
        "host": request.config.getoption("--db_host"),
        "port": request.config.getoption("--db_port"),
        "dbname": request.config.getoption("--db_name"),
        "user": user,
        "password": password
    }

@pytest.fixture(scope="session")
def data_quality_library():
    try:
        dq_lib = DataQualityLibrary()
        yield dq_lib
    except Exception as e:
        pytest.fail(f"Failed to initialize DataQualityLibrary: {e}")
    finally:
        del dq_lib

@pytest.fixture(scope="session")
def parquet_reader():
    try:
        reader = ParquetReader()
        yield reader
    except Exception as e:
        pytest.fail(f"Failed to initialize ParquetReader: {e}")
    finally:
        del reader

@pytest.fixture(scope="session")
def db_connection(db_credentials):
    try:
        with PostgresConnectorContextManager(
            db_user=db_credentials["user"],
            db_password=db_credentials["password"],
            db_host=db_credentials["host"],
            db_name=db_credentials["dbname"],
            db_port=db_credentials["port"]
        ) as connector:
            yield connector
    except Exception as e:
        pytest.fail(f"Failed to initialize DB connection: {e}")

@pytest.fixture(scope="module")
def target_data(parquet_reader):
    target_path = os.path.join(os.getcwd(), "parquet_output")
    try:
        data = parquet_reader.process(target_path, include_subfolders=True)
        return data
    except Exception as e:
        pytest.fail(f"Failed to load target parquet data: {e}")
