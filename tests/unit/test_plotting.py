import pandas as pd
import pytest
from matplotlib import pyplot as plt

from cromonitor.plotting import plotting


class TestPlotting:

    def test_calculate_workflow_duration(self, mock_data):
        """
        Test the calculate_workflow_duration function
        :param mock_data:
        :return:
        """

        # check the mock_data has dataframe called metrics

        # Call the function with the mock DataFrame
        result = plotting.calculate_workflow_duration(mock_data)

        # Assert that the result is as expected
        assert result == 171

    def test_get_task_summary(self, mock_data):
        """
        Test the get_task_summary function
        :param mock_data:
        :return:
        """

        # Call the function with the mock DataFrame
        task_summary_df, duration_sum = plotting.get_task_summary(mock_data)
        data = {"Tasks": ["write_to_stdout"], "Duration": [171], "Shards": [1]}

        # Create the DataFrame
        expected_df = pd.DataFrame(data)

        # Assert that the result is as expected
        pd.testing.assert_frame_equal(task_summary_df, expected_df)
        assert duration_sum == {"write_to_stdout": 171}

    @pytest.fixture
    def subplot(self):
        fig, ax = plt.subplots()
        return ax

    @pytest.fixture
    def df_monitoring_task_shard(self):
        return pd.DataFrame(
            {
                "metrics_timestamp": pd.date_range(start="1/1/2021", periods=5),
            }
        )

    @pytest.fixture
    def resource_used_array(self):
        return [10, 20, 30, 40, 50]

    @pytest.fixture
    def runtime_dic(self):
        return {
            "available_cpu_cores": 4,
            "available_mem_gb": 16,
            "available_disk_gb": 100,
            "requested_cpu_cores": 2,
            "requested_mem_gb": 8,
            "requested_disk_gb": 50,
        }

    def test_subplot_resource_usage(
        self, subplot, df_monitoring_task_shard, resource_used_array, runtime_dic
    ):
        result = plotting.subplot_resource_usage(
            subplot=subplot,
            df_monitoring_task_shard=df_monitoring_task_shard,
            resource_used_array=resource_used_array,
            runtime_dic=runtime_dic,
            task_shard_duration=100,
            resource_label="CPU",
            y_label="CPU % Used",
            obtained_resource_key="available_cpu_cores",
            requested_resource_key="requested_cpu_cores",
            available_resource=4.0,
        )
        assert isinstance(result, plt.Axes)
