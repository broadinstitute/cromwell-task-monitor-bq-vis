from datetime import datetime, timedelta

import pytest

from cromonitor.query import cost


class TestCost:
    @pytest.mark.parametrize(
        "end_time, expected",
        [
            (datetime.now() - timedelta(hours=23), False),
            (datetime.now() - timedelta(hours=25), True),
        ],
    )
    def test_check_minimum_time_passed_since_workflow_completion(
        self,
        end_time: datetime,
        expected: bool,
    ):
        # Act
        result = cost.check_minimum_time_passed_since_workflow_completion(end_time)

        # Assert
        assert result[0] == expected
