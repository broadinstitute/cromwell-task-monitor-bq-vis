from datetime import datetime, timedelta

import pandas as pd

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
