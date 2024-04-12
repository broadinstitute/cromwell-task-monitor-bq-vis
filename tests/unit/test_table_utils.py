import pandas as pd

from cromonitor.table import utils


class TestTablesUtils:
    def test_load_dataframe(self, mock_metrics_table, mock_data):
        # Use the load_dataframe function to load the DataFrame from the pickle file
        loaded_df = utils.load_dataframe(mock_metrics_table)

        # Assert that the loaded DataFrame is equal to the original DataFrame
        pd.testing.assert_frame_equal(loaded_df, mock_data.metrics)

    def test_get_info_per_task(self):
        # Create a mock DataFrame
        df = pd.DataFrame(
            {
                "runtime_task_call_name": ["task1", "task1", "task1", "task2"],
                "metrics_duration_sec": [70, 70, 70, 20],
                "runtime_shard": [1, 2, 3, 1],
            }
        )

        # Call the function with a task name and the mock DataFrame
        task_duration, shard_count = utils.get_info_per_task("task1", df)

        # Assert that the task duration and shard count are as expected
        assert task_duration == 70
        assert shard_count == 3

    def test_get_task_summary(self):
        # Create a mock DataFrame
        df = pd.DataFrame(
            {
                "runtime_task_call_name": ["task1", "task1", "task1", "task2"],
                "metrics_duration_sec": [70, 70, 70, 20],
                "runtime_shard": [1, 2, 3, 1],
            }
        )

        # Call the function with a list of task names and the mock DataFrame
        task_summary_dict = utils.get_task_summary(["task1", "task2"], df)

        # Assert that the task summary dictionary is as expected
        assert task_summary_dict == {"task1": (70, 3), "task2": (20, 1)}

    def test_get_task_summary_duration(self):
        # Create a mock task summary dictionary
        task_summary_dict = {"task1": (10, 1), "task2": (0, 1)}

        # Call the function with the mock task summary dictionary
        result = utils.get_task_summary_duration(task_summary_dict)

        # Assert that the result is as expected
        assert result == {"task1": 10, "task2": 0}

    def test_create_metrics_runtime_table(self, mock_data):

        loaded_metrics_runtime = utils.create_metrics_runtime_table(
            mock_data.metrics, mock_data.metadata_runtime
        )

        # Assert that the loaded DataFrame is equal to the original DataFrame
        pd.testing.assert_frame_equal(loaded_metrics_runtime, mock_data.metrics_runtime)
