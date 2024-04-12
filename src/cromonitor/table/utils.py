"""
This file contains utility functions for the data analysis
"""

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

    print(
        f"No data found. The file {filename} "
        f"does not exist in the current directory."
    )
    return None


def get_info_per_task(
    task_name: str, df: pd.DataFrame, task_column_name: str = "runtime_task_call_name"
) -> (int, int):
    """
    Get the duration and shard counts for a given task
    @param task_name: Name of the task
    @param df: Dataframe that contains the task information
    @param task_column_name: Name of the column that contains the task name
    @return:
    """
    df_task = df.loc[(df[task_column_name] == task_name)]
    task_duration = df_task.metrics_duration_sec.unique()[0]
    shard_count = len(df_task.runtime_shard.unique())
    return task_duration, shard_count


def get_task_summary(task_names: list, df: pd.DataFrame) -> dict:
    """
    Get the duration and shard counts for a given task
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
