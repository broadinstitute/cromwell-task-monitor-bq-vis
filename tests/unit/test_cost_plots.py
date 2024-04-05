import pandas as pd
import pytest

from cromonitor.plotting.cost_plots import (
    filter_and_sort_dataframe_by_cost,
    filter_dataframe_by_cost_threshold,
    get_sorted_group_order,
)


class TestCostPlots:

    @pytest.mark.parametrize(
        "threshold_cost_percent, expected",
        [
            (None, [400, 300, 200, 100]),  # Test case without threshold
            (50, [400]),  # Test case with threshold
        ],
    )
    def test_filter_and_sort_dataframe_by_cost(self, threshold_cost_percent, expected):
        # Create a test DataFrame
        data = {
            "task_name": ["task1", "task2", "task3", "task4"],
            "cost": [100, 200, 300, 400],
        }
        df = pd.DataFrame(data)

        # Call the function with the test DataFrame
        sorted_df = filter_and_sort_dataframe_by_cost(
            cost_df=df,
            grouping_column="task_name",
            threshold_cost_percent=threshold_cost_percent,
        )

        # Assert that the returned DataFrame is sorted in descending order and filtered correctly
        assert list(sorted_df["cost"]) == expected

    def test_filter_dataframe_by_cost_threshold(self):
        # Create a test DataFrame
        data = {
            "task_name": ["task1", "task2", "task3", "task4"],
            "cost": [100, 200, 300, 400],
        }
        df = pd.DataFrame(data)

        # Call the function with the test DataFrame and a threshold percentage
        threshold_percent = 50  # 50%
        filtered_df = filter_dataframe_by_cost_threshold(df, threshold_percent)

        # Calculate the total cost in the original DataFrame
        total_cost = df["cost"].sum()

        # Calculate the total cost in the filtered DataFrame
        filtered_total_cost = filtered_df["cost"].sum()

        # Assert that the total cost in the filtered DataFrame is less than or equal to the threshold percentage
        assert filtered_total_cost <= total_cost * (threshold_percent / 100)

        # Assert that the tasks in the filtered DataFrame are the ones with the highest cost
        assert set(filtered_df["task_name"]) == set(["task4"])

    def test_get_sorted_group_order(self):
        # Create a test DataFrame
        data = {
            "task_name": ["task1", "task2", "task3", "task4"],
            "cost": [100, 200, 300, 400],
        }
        df = pd.DataFrame(data)

        # Call the function with the test DataFrame
        sorted_index = get_sorted_group_order(
            cost_df_sorted=df,
            column_name_to_group_by="task_name",
            cost_column="cost",
        )

        # Assert that the returned index is sorted in descending order
        assert list(sorted_index) == ["task4", "task3", "task2", "task1"]
