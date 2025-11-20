import pytest
import os

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
