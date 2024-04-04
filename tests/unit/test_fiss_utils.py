from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pytest

from cromonitor.fiss import utils
from cromonitor.fiss.utils import Workflow


class TestFissUtils:
    def test_process_fapi_response_df(self):
        # Arrange
        df = pd.DataFrame(
            {
                "date": ["2022-01-01", "2022-01-02", "2022-01-03"],
                "value": [1, 2, 3],
                "other": ["a", "b", "c"],
            }
        )
        date_columns = ["date"]
        columns = ["date", "value"]
        sort_by = "date"

        # Act
        result = utils.process_fapi_response_df(df, date_columns, columns, sort_by)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert result.columns.tolist() == columns
        assert result.sort_values(by=sort_by, ascending=False).equals(result)

    def test_convert_to_datetime(self):
        # Arrange
        date_string = "2022-01-01T00:00:00.000Z"
        expected_datetime = datetime(2022, 1, 1, 0, 0, 0).replace(tzinfo=timezone.utc)

        # Act
        result = utils._convert_to_datetime(date_string)

        # Assert
        assert isinstance(result, datetime)
        assert result == expected_datetime

    def test_days_from_today(self):
        # Arrange
        target_date = datetime.now() - timedelta(days=5)

        # Act
        result = utils._days_from_today(target_date)

        # Assert
        assert result == 5

    @patch("cromonitor.fiss.utils.get_workflow_metadata_api_call")
    @pytest.mark.parametrize(
        "days, expected",
        [
            (0, -7),
            (1, -6),
        ],
    )
    def test_get_query_end_time(
        self, mock_get_workflow_metadata_api_call, days, expected
    ):
        class MockResponse:
            def __init__(self, status_code, json_data):
                self.status_code = status_code
                self.json_data = json_data

            def json(self):
                return self.json_data

        # Date
        # Subtract 14 days from the current date and time
        date_14_days_ago = datetime.now() - timedelta(days=days)

        # Format the date in the desired format
        formatted_date = date_14_days_ago.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        mock_get_workflow_metadata_api_call.return_value = MockResponse(
            status_code=200, json_data={"start": f"{formatted_date}", "end": None}
        )
        workflow = Workflow(
            workspace_namespace="namespace",
            workspace_name="workspace",
            submission_id="submission_id",
            parent_workflow_id="workflow_id",
        )

        # Act
        result = workflow._get_query_end_time()

        # Assert
        assert result == expected
