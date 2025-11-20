import pytest, os
import pandas as pd


# Fixture to provide absolute path to the CSV file
@pytest.fixture(scope="session")
def file_path():
    # Get the project root (two levels up from this file)
    base_dir = os.path.dirname(os.path.dirname(__file__))
    return os.path.join(base_dir, "src", "data", "data.csv")

# Fixture to read the CSV file
@pytest.fixture(scope="session")
def df(file_path):
    return pd.read_csv(file_path)

# Fixture to validate the schema of the file
@pytest.fixture(scope="session")
def test_validate_schema(df, validate_schema):
    expected_schema = ['id', 'name', 'age', 'email', 'is_active']
    actual_schema = df.columns.tolist()
    assert validate_schema(actual_schema, expected_schema), f"Schema does not match! Expected {expected_schema}, got {actual_schema}"

# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(items):
    for item in items:
        user_marks = [mark for mark in item.own_markers if mark.name not in ("skip", "xfail")]
        if not user_marks:
            item.add_marker(pytest.mark.unmarked)
