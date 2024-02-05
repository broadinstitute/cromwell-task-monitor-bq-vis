import numpy as np
import pandas as pd
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


def create_outlier_table_plotly(shards: list, resource_value: list, resource_label: str):
    """
    Create plotly tables for upper and lower outliers
    @param shards:
    @param resource_value:
    @param resource_label:
    @return:
    """
    (lower_outliers, upper_outliers) = get_outliers(
        shards=shards, resource_value=resource_value, resource_label=resource_label
    )

    upper_table = go.Table(
        header=dict(values=list(upper_outliers.columns),
                    fill_color='paleturquoise',
                    align='left'),
        cells=dict(values=[upper_outliers[col] for col in upper_outliers.columns],
                   fill_color='lavender',
                   align='left'))

    lower_table = go.Table(
        header=dict(values=list(lower_outliers.columns),
                    fill_color='paleturquoise',
                    align='left'),
        cells=dict(values=[lower_outliers[col] for col in lower_outliers.columns],
                   fill_color='lavender',
                   align='left'))

    return upper_table, lower_table
