import pytest
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.data_quality.data_quality_validation_library import DataQualityLibrary
from src.connectors.file_system.parquet_reader import ParquetReader
import os

db_user = os.getenv("POSTGRES_SECRET_USR")
db_password = os.getenv("POSTGRES_SECRET_PSW")

def pytest_addoption(parser):
    parser.addoption("--db_host", action="store", default="localhost", help="Database host")

def pytest_configure(config):
    """
    Validates that all required command-line options are provided.
    """
    required_options = [
        "--db_user", "--db_password"
    ]
    for option in required_options:
        if not config.getoption(option):
            pytest.fail(f"Missing required option: {option}")

@pytest.fixture(scope='session')
def db_connection(request):
    ...
    try:
        with PostgresConnectorContextManager(...) as db_connector:
            yield db_connector
    except Exception as e:
        pytest.fail(f"Failed to initialize PostgresConnectorContextManager: {e}")

...
