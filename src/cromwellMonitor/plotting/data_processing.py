import numpy as np
import pandas as pd
from ..logging import logging as log
import plotly.graph_objects as go


def mean_of_string(x: list):
    """
    Get the mean of a list of strings
    @param x:
    @return:
    """
    xfloat = np.array(x).astype(float)
    return np.nanmean(xfloat)


def get_1st_disk_usage(x):
    """
    Get the first disk usage
    @param x:
    @return:
    """
    xfloat = np.array(x).astype(float)
    return xfloat[0]


def get_outliers(
        shards: list, resource_value: list, resource_label: str
) -> (pd.DataFrame, pd.DataFrame):
    """
    Get the upper and lower outliers for a given resource
    @param shards:
    @param resource_value:
    @param resource_label:
    @return:
    """
    df = pd.DataFrame(dict(Resource_Usage=resource_value, Shard_Index=shards))

    # Find the quartiles and IQR
    q1 = df.Resource_Usage.quantile(q=0.25)
    q2 = df.Resource_Usage.quantile(q=0.5)
    q3 = df.Resource_Usage.quantile(q=0.75)
    iqr = q3 - q1
    upper = q3 + 1.5 * iqr
    lower = q1 - 1.5 * iqr

    upper_outliers = df[df.Resource_Usage > upper]
    lower_outliers = df[df.Resource_Usage < lower]

    # Rename the columns
    upper_outliers = upper_outliers.rename(columns={"Resource_Usage": resource_label})
    lower_outliers = lower_outliers.rename(columns={"Resource_Usage": resource_label})

    # Add a None row if there are no outliers
    if upper_outliers.empty:
        upper_outliers = pd.concat([upper_outliers, pd.DataFrame(
            [{resource_label: "None", "Shard_Index": "None"}])], ignore_index=True)

    if lower_outliers.empty:
        lower_outliers = pd.concat([lower_outliers, pd.DataFrame(
            [{resource_label: "None", "Shard_Index": "None"}])], ignore_index=True)

    return lower_outliers, upper_outliers


def fill_na_with_zero(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Function to replace NaN values with 0 in the specified columns of a DataFrame.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    columns (list): The list of columns in which to replace NaN values.

    Returns:
    pd.DataFrame: A copy of the input DataFrame with NaN values replaced with 0 in the specified columns.
    """

    # print warning if na values are present
    if df.isna().sum().sum() > 0:
        log.handle_value_warning(err="", message="NA values present in the dataframe. "
                       "Replacing with 0 for plotting requirements.")

    # Removes shards with null in columns being measured.
    df_filled = df.copy()
    for col in columns:
        df_filled[col] = df_filled[col].fillna(0)

    # error if datafram is empty
    if df_filled.empty:
        log.handle_value_error(err="Dataframe is empty")

    return df_filled


def remove_nan(input_data: dict):
    """
    Remove nan values from a dictionary
    @param input_data:
    @return:
    """
    clean_dict = {k: input_data[k] for k in input_data if not np.isnan(input_data[k])}
    return clean_dict


def calculate_shard_metrics(summary_shards, metrics_runtime, task_name_input, mean_of_string):
    """
    For each element in seres get the average cpu, max cpu, max mem, max disk
    # usage from monitoring datafram put in a dictionary
    :param summary_shards:
    :param metrics_runtime:
    :param task_name_input:
    :param mean_of_string:
    :return:
    """
    average_cpu_per_shard_dict = {}
    max_cpu_per_shard_dict = {}
    max_memory_per_shard_dict = {}
    max_disk_per_shard_dict = {}
    duration_per_shard_dict = {}

    for shard in summary_shards:
        # create dataframe for a given task name and shard
        df_summary_shard = metrics_runtime.loc[
            (metrics_runtime['runtime_task_call_name'] == task_name_input) & (
                    metrics_runtime['runtime_shard'] == shard)]

        cpu_time_mean = df_summary_shard.metrics_cpu_used_percent.apply(mean_of_string)

        average_cpu_per_shard_dict[str(shard)] = cpu_time_mean.mean()
        max_cpu_per_shard_dict[str(shard)] = cpu_time_mean.max()

        max_memory_per_shard_dict[str(shard)] = df_summary_shard.metrics_mem_used_gb.max()

        max_disk_per_shard_dict[str(shard)] = df_summary_shard.metrics_disk_used_gb.apply(
            get_1st_disk_usage).max()

        duration_per_shard_dict[str(shard)] = df_summary_shard['meta_duration_sec'].iloc[0]
        duration_per_shard_dict = {k: v if pd.notna(v) else 0 for k, v in duration_per_shard_dict.items()}

    return remove_nan(average_cpu_per_shard_dict), remove_nan(max_cpu_per_shard_dict), remove_nan(max_memory_per_shard_dict), remove_nan(max_disk_per_shard_dict), remove_nan(duration_per_shard_dict)

