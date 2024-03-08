import datetime
from os import path

import pandas as pd


def load_dataframe(filename) -> pd.DataFrame or None:
    """
    Load a dataframe from a pickle file
    :param filename:  the name of the pickle file
    :return:
    """
    if path.exists(filename):
        df = pd.read_pickle(filename)
        print(
            f"Successfully loaded data from {filename}. "
            f"The dataframe has {df.shape[0]} rows and {df.shape[1]} columns."
        )
        return df
    else:
        print(
            f"No data found. The file {filename} "
            f"does not exist in the current directory."
        )
        return None


def get_info_per_task(task_name, df) -> (int, int):
    """
    Get the duration and shard count for a given task
    @param task_name:
    @param df:
    @return:
    """
    df_task = df.loc[(df["runtime_task_call_name"] == task_name)]
    latest_end_datetime = max(df_task["metrics_timestamp"])
    earliest_start_datetime = min(df_task["metrics_timestamp"])
    task_duration = round(
        datetime.timedelta.total_seconds(latest_end_datetime - earliest_start_datetime)
    )

    shard_count = len(df_task.runtime_shard.unique())
    return task_duration, shard_count


def get_task_summary(task_names: list, df: pd.DataFrame) -> dict:
    """
    Get the duration and shard count for a given task
    :param df:
    :param task_names:
    :return:
    """
    task_summary_dict = {}
    for task in task_names:
        task_summary_dict[task] = get_info_per_task(task, df)
    return task_summary_dict


def get_task_summary_duration(task_summary_dict: dict) -> dict:
    """
    Get the duration and shard count for a given task
    :param task_summary_dict:
    :return:
    """
    task_summary_duration = {}
    for task in task_summary_dict:
        task_summary_duration[task] = task_summary_dict[task][0]
    return task_summary_duration


def create_metrics_runtime_table(metrics: pd.DataFrame, metadata_runtime: pd.DataFrame):
    """
    Create a table that merges metrics and runtime metadata
    @param metrics:
    @param metadata_runtime:
    @return:
    """
    metrics_runtime = pd.merge(
        metrics,
        metadata_runtime[
            [
                "runtime_workflow_id",
                "runtime_task_call_name",
                "runtime_shard",
                "runtime_instance_id",
                "metrics_duration_sec",
                "meta_duration_sec",
            ]
        ],
        left_on="metrics_instance_id",
        right_on="runtime_instance_id",
    )
    return metrics_runtime
