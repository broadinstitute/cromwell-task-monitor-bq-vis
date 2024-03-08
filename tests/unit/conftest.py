from pathlib import Path

import pandas as pd
import pytest

# "The conftest.py file serves as a means of providing fixtures for an entire directory.
# Fixtures defined in a conftest.py can be used by any test in that package without
# needing to import them (pytest will automatically discover them)."
# https://docs.pytest.org/en/latest/reference/fixtures.html


@pytest.fixture
def mock_data_path():
    return Path(__file__).parents[1].joinpath("mock_data/")


@pytest.fixture
def mock_metadata_runtime_table(mock_data_path):
    return mock_data_path.joinpath(
        "5f52afbb-28a5-4a1f-8cc6-60d3af22a625_metadata_runtime_resource_monitoring.pkl"
    )


@pytest.fixture
def mock_metrics_table(mock_data_path):
    return mock_data_path.joinpath(
        "5f52afbb-28a5-4a1f-8cc6-60d3af22a625_metrics_resource_monitoring.pkl"
    )


@pytest.fixture
def mock_metrics_runtime_table(mock_data_path):
    return mock_data_path.joinpath(
        "5f52afbb-28a5-4a1f-8cc6-60d3af22a625_metrics_runtime_resource_monitoring.pkl"
    )


@pytest.fixture
def mock_data(
    mock_metadata_runtime_table, mock_metrics_table, mock_metrics_runtime_table
):
    class MockData:
        def __init__(self):
            self.metadata_runtime = pd.read_pickle(mock_metadata_runtime_table)
            self.metrics = pd.read_pickle(mock_metrics_table)
            self.metrics_runtime = pd.read_pickle(mock_metrics_runtime_table)

    return MockData()
