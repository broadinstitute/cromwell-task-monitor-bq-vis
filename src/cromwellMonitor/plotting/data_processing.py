import numpy as np
import pandas as pd
from bokeh.models import ColumnDataSource, TableColumn, DataTable, Div


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
        shards: np.ndarray, resource_value: list, resource_label: str
) -> (pd.DataFrame, pd.DataFrame):
    """
    Get the upper and lower outliers for a given resource
    @param shards:
    @param resource_value:
    @param resource_label:
    @return:
    """
    df = pd.DataFrame(dict(Resource_Usage=resource_value, Shard_Index=shards))

    # find the quartiles and IQR
    q1 = df.Resource_Usage.quantile(q=0.25)
    q2 = df.Resource_Usage.quantile(q=0.5)
    q3 = df.Resource_Usage.quantile(q=0.75)
    iqr = q3 - q1
    upper = q3 + 1.5 * iqr
    lower = q1 - 1.5 * iqr

    upper_outliers = df[df.Resource_Usage > upper]
    lower_outliers = df[df.Resource_Usage < lower]

    upper_outliers = upper_outliers.rename(columns={"Resource_Usage": resource_label})
    lower_outliers = lower_outliers.rename(columns={"Resource_Usage": resource_label})

    # print("Upper outliers: ")
    # display(upper_outliers)
    if upper_outliers.empty:
        upper_outliers = pd.concat([upper_outliers, pd.DataFrame(
            [{resource_label: "None", "Shard_Index": "None"}])], ignore_index=True)

    # print("Lower outliers: ")
    # display(lower_outliers)
    if lower_outliers.empty:
        lower_outliers = pd.concat([lower_outliers, pd.DataFrame(
            [{resource_label: "None", "Shard_Index": "None"}])], ignore_index=True)

    return lower_outliers, upper_outliers


def create_outlier_table(
        shards: np.ndarray, resource_value: list, resource_label: str
):
    """
    Create bokeh tables for upper and lower outliers
    @param shards:
    @param resource_value:
    @param resource_label:
    @return:
    """
    (lower_outliers, upper_outliers) = get_outliers(
        shards=shards, resource_value=resource_value, resource_label=resource_label
    )

    # Get Column names from pandas dataframe
    Columns = [TableColumn(field=Ci, title=Ci) for Ci in upper_outliers.columns]

    # Create bokeh datatables using pandas dataframe and column names
    upper_table = DataTable(
        columns=Columns, source=ColumnDataSource(upper_outliers)
    )  # bokeh table
    lower_table = DataTable(
        columns=Columns, source=ColumnDataSource(lower_outliers)
    )  # bokeh table

    # table title
    upper_div = Div(text="<h3>Upper Outliers</h3>", width=200, height=20)
    lower_div = Div(text="<h3>Lower Outliers</h3>", width=200, height=20)

    return upper_div, upper_table, lower_div, lower_table
