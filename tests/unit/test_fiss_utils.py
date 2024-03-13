from datetime import datetime, timedelta

import pandas as pd
import pytest

from cromonitor.fiss import utils


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
        expected_datetime = datetime(2022, 1, 1, 0, 0, 0)

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

    @pytest.mark.parametrize(
        "workflow_metadata, expected_end_time",
        [
            # Test case 1: workflow_metadata contains "end" key
            (
                {
                    "start": "2022-01-01T00:00:00.000Z",
                    "end": "2022-01-02T00:00:00.000Z",
                },
                datetime.strptime("2022-01-02T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
            ),
            # Test case 2: workflow_metadata does not contain "end" key
            (
                {"start": "2022-01-01T00:00:00.000Z"},
                datetime.strptime("2022-01-01T23:59:59.999Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
            ),
        ],
    )
    def test_get_workflow_end_time(self, workflow_metadata, expected_end_time):
        # Create a Workflow instance
        # workflow = utils.Workflow(
        #     workspace_namespace="workspace_namespace",
        #     workspace_name="workspace_name",
        #     submission_id="submission_id",
        #     parent_workflow_id="parent_workflow_id"
        # )

        assert (
            utils.Workflow._get_workflow_end_time(
                self, workflow_metadata=workflow_metadata
            )
            == expected_end_time
        )
