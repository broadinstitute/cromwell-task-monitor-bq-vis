import pandas as pd

from cromwellMonitor.plotting import plotting


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
