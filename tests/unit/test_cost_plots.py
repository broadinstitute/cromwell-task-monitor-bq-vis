import pandas as pd

from src.cromonitor.plotting.cost_plots import filter_dataframe_by_cost_threshold


def test_filter_dataframe_by_cost_threshold():
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
