import datetime
import logging
from typing import List, Optional, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import seaborn as sns
from plotly.subplots import make_subplots

from ..table import utils as tableUtils
from .data_processing import (
    calculate_shard_metrics,
    fill_na_with_zero,
    get_outliers,
    mean_of_string,
)

logger = logging.getLogger(__name__)


def calculate_workflow_duration(df_monitoring) -> int:
    """
    Calculate the total duration of a workflow in seconds.

    Parameters:
    df_monitoring (DataFrame): The dataframe containing the monitoring metrics.

    Returns:
    int: The total duration of the workflow in seconds.
    """
    latest_end_datetime = max(df_monitoring.metrics["metrics_timestamp"])
    earliest_start_datetime = min(df_monitoring.metrics["metrics_timestamp"])
    workflow_duration: int = round(
        datetime.timedelta.total_seconds(latest_end_datetime - earliest_start_datetime)
    )

    return workflow_duration


def get_sorted_task_summary(
    df_monitoring: pd.DataFrame,
    task_column_name: str = "Tasks",
    duration_column_name: str = "Duration(s)",
    shards_column_name: str = "Shards",
) -> (pd.DataFrame, dict):
    """
    Get the task name and duration summary for a given workflow monitoring dataframe

    :param df_monitoring: The dataframe that contains the monitoring metrics.
    :param shards_column_name: Shard column name for the task summary table
    :param duration_column_name: Duration column name for the task summary table
    :param task_column_name: Task column name for the task summary table

    :return:
    """
    # Sort the task summary by duration
    df_monitoring.metrics_runtime.sort_values(
        by="metrics_duration_sec", ascending=False, inplace=True
    )

    all_task_names = df_monitoring.metrics_runtime.runtime_task_call_name.unique()
    task_summary_dict = tableUtils.get_task_summary(
        task_names=all_task_names, df=df_monitoring.metrics_runtime
    )
    task_summary_duration = tableUtils.get_task_summary_duration(
        task_summary_dict=task_summary_dict
    )

    # Convert the task summary dictionary to a dataframe
    df_task_summary = pd.DataFrame.from_dict(task_summary_dict)
    df_task_summary_T = df_task_summary.T.rename_axis(task_column_name).reset_index()
    df_task_summary_named = df_task_summary_T.rename(
        columns={0: duration_column_name, 1: shards_column_name}, errors="raise"
    )

    return df_task_summary_named, task_summary_duration


def create_plotly_table(df_input: pd.DataFrame):
    """
    Create a plotly table from a given dataframe
    :param df_input: The dataframe to be converted to a plotly table
    :return:
    """
    return go.Table(
        header=dict(
            values=list(df_input.columns), fill_color="paleturquoise", align="left"
        ),
        cells=dict(
            values=[df_input[col] for col in df_input.columns],
            fill_color="lavender",
            align="left",
        ),
    )


def create_outlier_table_plotly(
    shards: list, resource_value: list, resource_label: str
):
    """
    Create plotly tables for upper and lower outliers
    @param shards: The list of shards
    @param resource_value: The list of resource values
    @param resource_label: The label of the resource
    @return:
    """
    (lower_outliers, upper_outliers) = get_outliers(
        shards=shards, resource_value=resource_value, resource_label=resource_label
    )

    upper_table = create_plotly_table(df_input=upper_outliers)

    lower_table = create_plotly_table(df_input=lower_outliers)

    return upper_table, lower_table


def create_workflow_summary_subplot_figure(
    workflow_task_summary_table: go.Table,
    workflow_duration_bar: go.Bar,
    parent_workflow_id: str,
):
    """
    Create a subplot figure for the workflow summary
    :param workflow_task_summary_table: The workflow task summary table
    :param workflow_duration_bar: The workflow duration bar plot
    :param parent_workflow_id: The parent workflow id
    :return:
    """
    fig = make_subplots(
        rows=2,
        cols=1,
        vertical_spacing=0.1,
        specs=[[{"type": "table"}], [{"type": "bar"}]],
        subplot_titles=["Workflow Summary Table", "Workflow Duration in Seconds"],
    )

    # Add plots to the subplots
    fig.add_trace(workflow_task_summary_table, row=1, col=1)
    fig.add_trace(workflow_duration_bar, row=2, col=1)

    # Update Plot X Axis Title
    fig.update_xaxes(title_text="Tasks", row=2, col=1)
    # Update Plot Y Axis Title
    fig.update_yaxes(title_text="Seconds", row=2, col=1)

    fig.update_layout(
        height=800,
        width=800,
        title_text="{} Workflow Summary".format(parent_workflow_id),
        showlegend=False,
    )

    return fig


def generate_workflow_summary(
    parent_workflow_id: str, df_monitoring: pd.DataFrame, make_pdf: bool = False
) -> go.Figure:
    """
    Generate a workflow summary html file using bokeh
    @param parent_workflow_id: The parent workflow id
    @param df_task_summary_named: The dataframe containing the task summary
    @param task_summary_duration: The task summary duration
    @param df_monitoring: The dataframe containing the monitoring metrics
    @return:
    """

    workflow_duration = calculate_workflow_duration(df_monitoring=df_monitoring)

    df_task_summary_named, task_summary_duration = get_sorted_task_summary(
        df_monitoring
    )

    # Create table using plotly
    workflow_task_summary_table = create_plotly_table(df_input=df_task_summary_named)

    # Ensure total duration is positive integer
    if workflow_duration < 0:
        logger.warning("Total duration is negative.")

    workflow_duration_bar = plot_bar_plotly_using_dict(
        data_dict=task_summary_duration,
        x_legend_label="Tasks",
        y_legend_label="Seconds",
        legend_title=f"Total Workflow Duration: {workflow_duration} seconds",
        show_legend=True,
    )

    fig = create_workflow_summary_subplot_figure(
        workflow_task_summary_table=workflow_task_summary_table,
        workflow_duration_bar=workflow_duration_bar,
        parent_workflow_id=parent_workflow_id,
    )

    if make_pdf:
        pio.write_image(
            fig, "{}_workflow_summary.pdf".format(parent_workflow_id), format="pdf"
        )

    return fig


def plot_bar_plotly(x_value: list, y_value: list, y_label: str, x_label: str):
    """
    Plot a bar plot using Plotly
    @param x_value:
    @param y_value:
    @param y_label:
    @param x_label:
    @return:
    """

    return go.Bar(x=x_value, y=y_value, x0=x_label, y0=y_label)


def plot_bar_plotly_using_dict(
    data_dict: dict,
    x_legend_label: str,
    y_legend_label: str,
    show_legend: bool = False,
    legend_title: str = None,
):
    """
    Generate a bar plot using plotly
    @param data_dict: The dictionary containing the data
    @param x_legend_label: The x axis label
    @param y_legend_label: The y axis label
    @param show_legend: Whether to show the legend
    @param legend_title: The title of the legend
    @return:
    """

    # Extract data from the dictionary
    x_values = list(data_dict.keys())
    y_values = list(data_dict.values())

    # Create bars with hover tooltips
    return go.Bar(
        x=x_values,
        y=y_values,
        hovertemplate=f"{x_legend_label}: %{{x}}<br>{y_legend_label}: %{{y:,.0f}}<br>",
        name=legend_title,
        showlegend=show_legend,
    )


def create_violin_plot_plotly(
    y_values: list, x_values: list, y_label: str, x_label: str
):
    """
    Create a violin plot with outliers marked using Plotly
    @param title: The title of the plot
    @param data: The data to be plotted
    @param y_label: The y axis label
    @param x_label: The x axis label
    @return:
    """
    # Create a DataFrame from the data
    df = pd.DataFrame({"y_value": y_values, "x_value": x_values})

    return go.Violin(
        y=df["y_value"],
        box_visible=True,
        line_color="black",
        meanline_visible=True,
        fillcolor="blue",
        opacity=0.6,
        points="outliers",
        hoverinfo="y",
        hovertemplate=f"<i>{x_label}</i>: %{{text}}<br>" + f"<i>{y_label}</i>: %{{y}}",
        text=df["x_value"].values,
    )


def generate_resource_plots_and_outliers(
    input_dataset: dict, y_label: str, x_label: str
):
    """
    Generate an outlier table, plot a violin plot and bar plot for a given resource
    @param input_dataset:
    @param title: The title of the plot
    @param y_label: The y axis label
    @param x_label: The x axis label
    @return:
    """

    x = list(input_dataset.keys())
    y = [round(value) for value in input_dataset.values()]
    (upper_outlier_table, lower_outlier_table) = create_outlier_table_plotly(
        shards=x, resource_value=y, resource_label=y_label
    )
    violin_plot = create_violin_plot_plotly(
        x_values=x, y_values=y, y_label=y_label, x_label=x_label
    )
    bar_plot = plot_bar_plotly(x_value=x, y_value=y, y_label=y_label, x_label=x_label)

    return bar_plot, violin_plot, lower_outlier_table, upper_outlier_table


def get_unique_shards_for_task(df: pd.DataFrame, task_name_input: str):
    """
    Get all unique runtime_shard values for a given task name
    from the runtime_metrics dataframe
    :param df: runtime_metrics dataframe
    :param task_name_input: Name of the task
    :return:
    """
    return df.runtime_shard.loc[
        (df["runtime_task_call_name"] == task_name_input)
    ].unique()


def plot_shard_summary(
    parent_workflow_id: str,
    metrics_runtime: pd.DataFrame,
    task_name_input: str,
    plt_height: int = 5000,
    plt_width: int = 1200,
):
    """
    Plot the shard summary for a given task name
    :param parent_workflow_id: The parent workflow id
    :param metrics_runtime: The dataframe containing the monitoring metrics
    :param task_name_input: The task name
    :param plt_height: Height of the plot
    :param plt_width: Width of the plot
    :return:
    """

    # print warning if na values are present
    df_filled = fill_na_with_zero(
        df=metrics_runtime,
        columns=[
            "metrics_duration_sec",
            "meta_duration_sec",
            "metrics_disk_used_gb",
            "metrics_mem_used_gb",
            "metrics_cpu_used_percent",
        ],
    )

    summary_shards = get_unique_shards_for_task(
        df=df_filled, task_name_input=task_name_input
    )

    (
        average_cpu_per_shard_clean_dict,
        max_cpu_per_shard_clean_dict,
        max_memory_per_shard_clean_dict,
        max_disk_per_shard_clean_dict,
        duration_per_shard_clean_dict,
    ) = calculate_shard_metrics(
        summary_shards=summary_shards,
        metrics_runtime=metrics_runtime,
        task_name_input=task_name_input,
        mean_of_string=mean_of_string,
    )

    # Sort resource dict by value
    average_cpu_per_shard_sorted_dict = {
        k: v
        for k, v in sorted(
            average_cpu_per_shard_clean_dict.items(),
            reverse=True,
            key=lambda item: item[1],
        )
    }
    max_cpu_per_shard_sorted_dict = {
        k: v
        for k, v in sorted(
            max_cpu_per_shard_clean_dict.items(), reverse=True, key=lambda item: item[1]
        )
    }
    max_memory_per_shard_sorted_dict = {
        k: v
        for k, v in sorted(
            max_memory_per_shard_clean_dict.items(),
            reverse=True,
            key=lambda item: item[1],
        )
    }
    max_disk_per_shard_sorted_dict = {
        k: v
        for k, v in sorted(
            max_disk_per_shard_clean_dict.items(),
            reverse=True,
            key=lambda item: item[1],
        )
    }
    duration_per_shard_sorted_dict = {
        k: v
        for k, v in sorted(
            duration_per_shard_clean_dict.items(),
            reverse=True,
            key=lambda item: item[1],
        )
    }

    # Generate plots and outliersf

    p_cpu_a = generate_resource_plots_and_outliers(
        input_dataset=average_cpu_per_shard_sorted_dict,
        y_label="CPU Usage",
        x_label="Shards",
    )
    p_cpu_m = generate_resource_plots_and_outliers(
        input_dataset=max_cpu_per_shard_sorted_dict,
        y_label="CPU Usage",
        x_label="Shards",
    )
    p_mem_m = generate_resource_plots_and_outliers(
        input_dataset=max_memory_per_shard_sorted_dict,
        y_label="Memory Usage GB",
        x_label="Shards",
    )
    p_dis_m = generate_resource_plots_and_outliers(
        input_dataset=max_disk_per_shard_sorted_dict,
        y_label="Disk Usage GB",
        x_label="Shards",
    )
    p_dur = generate_resource_plots_and_outliers(
        input_dataset=duration_per_shard_sorted_dict,
        y_label="Seconds",
        x_label="Shards",
    )

    # Writes to html file all the generated plots and tables for summary shard
    fig = make_subplots(
        rows=10,
        cols=2,
        vertical_spacing=0.03,
        specs=[
            [{"type": "bar"}, {"type": "scatter"}],
            [{"type": "table"}, {"type": "table"}],
            [{"type": "bar"}, {"type": "scatter"}],
            [{"type": "table"}, {"type": "table"}],
            [{"type": "bar"}, {"type": "scatter"}],
            [{"type": "table"}, {"type": "table"}],
            [{"type": "bar"}, {"type": "scatter"}],
            [{"type": "table"}, {"type": "table"}],
            [{"type": "bar"}, {"type": "scatter"}],
            [{"type": "table"}, {"type": "table"}],
        ],
        subplot_titles=[
            "Average CPU Usage Per Shard",
            "Average CPU Usage Percentage",
            "Average CPU Usage Lower Outliers",
            "Average CPU Usage Upper Outliers",
            "Max CPU Usage Per Shard",
            "Max CPU Usage Percentage",
            "Max CPU Usage Lower Outliers",
            "Max CPU Usage Upper Outliers",
            "Max Memory Usage Per Shard",
            "Max Memory Usage",
            "Max Memory Usage Lower Outliers",
            "Max Memory Usage Upper Outliers",
            "Max Disk Usage Per Shard",
            "Max Disk Usage",
            "Max Disk Usage Lower Outliers",
            "Max Disk Usage Upper Outliers",
            "Time Duration Usage Per Shard",
            "Time Duration" "Time Duration Lower Outliers",
            "Time Duration Upper Outliers",
        ],
    )

    # Add plots to the subplots
    fig.add_trace(p_cpu_a[0], row=1, col=1)  # Table
    fig.add_trace(p_cpu_a[1], row=1, col=2)  # Table
    fig.add_trace(p_cpu_a[2], row=2, col=1)  # Plot
    fig.add_trace(p_cpu_a[3], row=2, col=2)  # Plot
    fig.add_trace(p_cpu_m[0], row=3, col=1)  # Table
    fig.add_trace(p_cpu_m[1], row=3, col=2)  # Table
    fig.add_trace(p_cpu_m[2], row=4, col=1)  # Plot
    fig.add_trace(p_cpu_m[3], row=4, col=2)  # Plot
    fig.add_trace(p_mem_m[0], row=5, col=1)  # Table
    fig.add_trace(p_mem_m[1], row=5, col=2)  # Table
    fig.add_trace(p_mem_m[2], row=6, col=1)  # Plot
    fig.add_trace(p_mem_m[3], row=6, col=2)  # Plot
    fig.add_trace(p_dis_m[0], row=7, col=1)  # Table
    fig.add_trace(p_dis_m[1], row=7, col=2)  # Table
    fig.add_trace(p_dis_m[2], row=8, col=1)  # Plot
    fig.add_trace(p_dis_m[3], row=8, col=2)  # Plot
    fig.add_trace(p_dur[0], row=9, col=1)  # Table
    fig.add_trace(p_dur[1], row=9, col=2)  # Table
    fig.add_trace(p_dur[2], row=10, col=1)  # Plot
    fig.add_trace(p_dur[3], row=10, col=2)  # Plot

    # Update X Axis Title
    fig.update_xaxes(title_text="Shards", row=1, col=1)
    fig.update_xaxes(title_text="Shards", row=1, col=2)
    fig.update_xaxes(title_text="Shards", row=3, col=1)
    fig.update_xaxes(title_text="Shards", row=3, col=2)
    fig.update_xaxes(title_text="Shards", row=5, col=1)
    fig.update_xaxes(title_text="Shards", row=5, col=2)
    fig.update_xaxes(title_text="Shards", row=7, col=1)
    fig.update_xaxes(title_text="Shards", row=7, col=2)
    fig.update_xaxes(title_text="Shards", row=9, col=1)
    fig.update_xaxes(title_text="Shards", row=9, col=2)

    # Update Y Axis Title
    fig.update_yaxes(title_text="CPU Usage", row=1, col=1)
    fig.update_yaxes(title_text="CPU Usage", row=1, col=2)
    fig.update_yaxes(title_text="CPU Usage", row=3, col=1)
    fig.update_yaxes(title_text="CPU Usage", row=3, col=2)
    fig.update_yaxes(title_text="Memory Usage", row=5, col=1)
    fig.update_yaxes(title_text="Memory Usage", row=5, col=2)
    fig.update_yaxes(title_text="Disk Usage", row=7, col=1)
    fig.update_yaxes(title_text="Disk Usage", row=7, col=2)
    fig.update_yaxes(title_text="Seconds", row=9, col=1)
    fig.update_yaxes(title_text="Seconds", row=9, col=2)

    # Add title to the whole figure
    fig.update_layout(
        height=plt_height,
        width=plt_width,
        title_text="{} {} Task Shard Summary".format(
            parent_workflow_id, task_name_input
        ),
        showlegend=False,
    )

    return fig


def subplot_resource_usage(
    subplot,
    df_monitoring_task_shard: pd.DataFrame,
    resource_used_array: list,
    runtime_dic: dict,
    task_shard_duration: int,
    resource_label: str,
    y_label: str,
    obtained_resource_key: Optional[str] = None,
    requested_resource_key: Optional[str] = None,
    available_resource: Optional[float] = None,
) -> plt.Axes:
    """
    Plots the usage of a specific resource on a given subplot.

    This function plots the usage of a resource such as CPU, memory, or disk space over time.
    It also plots lines indicating the obtained and requested amounts of the resource, if provided.

    @param subplot: The plot to draw on.
    @param df_monitoring_task_shard: The dataframe containing the monitoring metrics.
    @param resource_used_array: The array of resource usage percentages.
    @param runtime_dic: The dictionary of runtime attributes.
    @param task_shard_duration: The duration of the task shard.
    @param resource_label: The label of the resource.
    @param obtained_resource_key: The key to get the obtained resource from the runtime dictionary.
    @param requested_resource_key: The key to get the requested resource from the runtime dictionary.
    """
    subplot.plot(
        df_monitoring_task_shard.metrics_timestamp.astype("O"),
        resource_used_array,
        label=f"{resource_label} Used",
    )
    if available_resource:
        subplot.axhline(
            y=available_resource,
            color="r",
            label=f"Max {resource_label}: %.2f" % available_resource,
        )
    if obtained_resource_key:
        subplot.plot(
            [],
            [],
            " ",
            label=f"Obtained {resource_label}: %s" % obtained_resource_key,
        )
    if requested_resource_key:
        subplot.plot(
            [],
            [],
            " ",
            label=f"Requested {resource_label}: {runtime_dic[requested_resource_key]}",
        )
    subplot.legend(loc="upper center", bbox_to_anchor=(1.20, 0.8), shadow=True, ncol=1)
    subplot.set_ylabel(f"{y_label}")
    subplot.set_xlabel("Date Time")
    subplot.set_xlim(
        df_monitoring_task_shard.metrics_timestamp.min(),
        df_monitoring_task_shard.metrics_timestamp.max(),
    )  # Set x-axis limits
    subplt2 = subplot.twiny()
    subplt2.set_xlim(0, task_shard_duration)
    subplt2.set_xlabel("Duration Time")
    subplot.grid(True)

    return subplot


def plot_detailed_resource_usage(
    task_name: str,
    shard_number: int,
    task_shard_duration: int,
    df_monitoring_task_shard: pd.DataFrame,
    cpu_used_percent_array: list,
    runtime_dic: dict,
    disk_used_gb_array: list,
    disk_read_iops_array: list,
    disk_write_iops_array: list,
    plt_height: int = 2000,
    plt_width: int = 1200,
) -> plt:
    """
    Creates a plot with resource usage figure for a given
    task and its shard using matplotlib.

    @param task_name:
    @param shard_number:
    @param task_shard_duration:
    @param df_monitoring_task_shard:
    @param cpu_used_percent_array:
    @param runtime_dic:
    @param disk_used_gb_array:
    @param disk_read_iops_array:
    @param disk_write_iops_array:
    @return:
    """
    # For size and style of plots
    dpi = 100
    fig, axs = plt.subplots(5, 1, figsize=(plt_width / dpi, plt_height / dpi), dpi=dpi)
    sns.set(style="whitegrid")

    cpu_plt = axs[0]
    cpu_plt.set_title(
        "Task Name: "
        + task_name
        + " Shard: "
        + str(shard_number)
        + " Duration: "
        + str(task_shard_duration),
        fontsize=20,
    )
    subplot_resource_usage(
        subplot=cpu_plt,
        df_monitoring_task_shard=df_monitoring_task_shard,
        resource_used_array=cpu_used_percent_array,
        runtime_dic=runtime_dic,
        task_shard_duration=task_shard_duration,
        resource_label="CPU",
        y_label="CPU % Used",
        obtained_resource_key=str(runtime_dic["available_cpu_cores"]),
        requested_resource_key="requested_cpu_cores",
    )

    mem_plt = axs[1]
    subplot_resource_usage(
        subplot=mem_plt,
        df_monitoring_task_shard=df_monitoring_task_shard,
        resource_used_array=df_monitoring_task_shard.metrics_mem_used_gb,
        runtime_dic=runtime_dic,
        task_shard_duration=task_shard_duration,
        resource_label="Memory",
        y_label="Memory GB Used",
        requested_resource_key="requested_mem_gb",
        available_resource=runtime_dic["available_mem_gb"],
    )

    disk_plt = axs[2]
    subplot_resource_usage(
        subplot=disk_plt,
        df_monitoring_task_shard=df_monitoring_task_shard,
        resource_used_array=disk_used_gb_array,
        runtime_dic=runtime_dic,
        task_shard_duration=task_shard_duration,
        resource_label="Disk",
        y_label="Disk GB Used",
        requested_resource_key="requested_disk_gb",
        available_resource=runtime_dic["available_disk_gb"],
    )

    disk_read_plt = axs[3]
    subplot_resource_usage(
        subplot=disk_read_plt,
        df_monitoring_task_shard=df_monitoring_task_shard,
        resource_used_array=disk_read_iops_array,
        runtime_dic=runtime_dic,
        task_shard_duration=task_shard_duration,
        resource_label="Disk Read_IOps",
        y_label="Disk Read_IOps",
    )

    disk_write_plt = axs[4]
    subplot_resource_usage(
        subplot=disk_write_plt,
        df_monitoring_task_shard=df_monitoring_task_shard,
        resource_used_array=disk_write_iops_array,
        runtime_dic=runtime_dic,
        task_shard_duration=task_shard_duration,
        resource_label="Disk Write_IOps",
        y_label="Disk Write_IOps",
    )

    return fig


def plot_shards(
    df_monitoring: pd.DataFrame,
    task_name: str,
    shards,
    plt_height: int = None,
    plt_width: int = None,
) -> plt:
    """
    Plot the shards for a given task name
    :param plt_width:
    :param plt_height:
    :param df_monitoring:
    :param task_name:
    :param shards:
    :param pdf:
    :return:
    """
    resource_plt = plt.figure()

    for shard in shards:
        df_monitoring_task_shard = df_monitoring.metrics_runtime.loc[
            (df_monitoring.metrics_runtime["runtime_task_call_name"] == task_name)
            & (df_monitoring.metrics_runtime["runtime_shard"] == shard)
        ]
        df_monitoring_metadata_runtime_task_shard = df_monitoring.metadata_runtime.loc[
            (df_monitoring.metadata_runtime["runtime_task_call_name"] == task_name)
            & (df_monitoring.metadata_runtime["runtime_shard"] == shard)
        ]
        df_monitoring_task_shard = df_monitoring_task_shard.sort_values(
            by="metrics_timestamp"
        )

        # Calculate the duration of the task shard
        max_datetime = max(df_monitoring_task_shard["metrics_timestamp"])
        min_datetime = min(df_monitoring_task_shard["metrics_timestamp"])
        task_shard_duration = round(
            datetime.timedelta.total_seconds(max_datetime - min_datetime)
        )

        # create an array for to hold the y values from the list columns
        cpu_used_percent_array = [
            np.asarray(x).mean()
            for x in df_monitoring_task_shard.metrics_cpu_used_percent
        ]
        disk_used_gb_array = [
            np.asarray(x).max() for x in df_monitoring_task_shard.metrics_disk_used_gb
        ]
        disk_read_iops_array = [
            np.asarray(x).max() for x in df_monitoring_task_shard.metrics_disk_read_iops
        ]
        disk_write_iops_array = [
            np.asarray(x).max()
            for x in df_monitoring_task_shard.metrics_disk_write_iops
        ]

        # Creates a dictionary of runtime attributes
        runtime_dic = create_runtime_dict(df_monitoring_metadata_runtime_task_shard)

        # Plotting
        # For size and style of plots
        resource_plt = plot_detailed_resource_usage(
            task_name=task_name,
            shard_number=shard,
            task_shard_duration=task_shard_duration,
            df_monitoring_task_shard=df_monitoring_task_shard,
            cpu_used_percent_array=cpu_used_percent_array,
            runtime_dic=runtime_dic,
            disk_used_gb_array=disk_used_gb_array,
            disk_read_iops_array=disk_read_iops_array,
            disk_write_iops_array=disk_write_iops_array,
            plt_height=plt_height,
            plt_width=plt_width,
        )

        resource_plt.subplots_adjust(hspace=0.5)

    return resource_plt


def plot_resource_usage(
    df_monitoring: pd.DataFrame,
    parent_workflow_id: str,
    task_names: List[str],
    plt_height: Optional[int] = None,
    plt_width: Optional[int] = None,
    target_shard: Optional[int] = None,
) -> Union[go.Figure, plt.Figure]:
    """
    Creates a pdf file with resource usage plots for each task name
    @param df_monitoring: The dataframe containing the monitoring metrics
    @param parent_workflow_id: The parent workflow id
    @param task_names: The task names
    @param plt_height: The height of the plot
    @param plt_width: The width of the plot
    @param target_shard: Specific shards to plot for a sharded/scattered task
    @return: A pdf file with resource usage plots for each task name
    """
    for task_name in task_names:
        if (
            task_name
            not in df_monitoring.metadata_runtime.runtime_task_call_name.unique()
        ):
            raise ValueError(f"Task name {task_name} not found in dataframe")

        all_shards = (
            df_monitoring.metadata_runtime.runtime_shard.loc[
                (df_monitoring.metadata_runtime["runtime_task_call_name"] == task_name)
            ]
            .sort_values()
            .unique()
        )

        if target_shard:
            if target_shard not in all_shards:
                raise ValueError(f"Shard {target_shard} not found in dataframe")

        task_shard_lookup: List[int] = (
            [target_shard] if target_shard is not None else all_shards
        )

        if len(task_shard_lookup) > 1:
            return plot_shard_summary(
                metrics_runtime=df_monitoring.metrics_runtime,
                task_name_input=task_name,
                parent_workflow_id=parent_workflow_id,
                plt_height=plt_height,
                plt_width=plt_width,
            )
        else:
            return plot_shards(
                df_monitoring=df_monitoring,
                task_name=task_name,
                shards=task_shard_lookup,
                plt_height=plt_height,
                plt_width=plt_width,
            )


def save_plot_as_pdf(plot: Union[go.Figure, plt.Figure], filename: str):
    if isinstance(plot, go.Figure):
        plot.write_image(filename, format="pdf")
    else:
        plot.savefig(filename, bbox_inches="tight", pad_inches=0.5, format="pdf")


def create_runtime_dict(
    df_monitoring_metadata_runtime_task_shard: pd.DataFrame,
) -> dict:
    """
    Creates a dictionary of runtime attributes
    @param df_monitoring_metadata_runtime_task_shard:
    @return:
    """

    first_shard = df_monitoring_metadata_runtime_task_shard.iloc[0]

    runtime_dic = {
        "available_cpu_cores": first_shard.at["runtime_cpu_count"],
        "available_mem_gb": first_shard.at["runtime_mem_total_gb"].round(2),
        "available_disk_gb": first_shard.at["runtime_disk_total_gb"][0].round(2),
        "requested_cpu_cores": first_shard.at["meta_cpu"],
        "requested_mem_gb": first_shard.at["meta_mem_total_gb"],
        # Todo: Fix the following line so its not returning None
        "requested_disk_gb": (
            None
            if np.isnan(first_shard.at["meta_disk_total_gb"])
            else (
                first_shard.at["meta_disk_total_gb"].round(2)
                if isinstance(first_shard.at["meta_disk_total_gb"], float)
                else first_shard.at["meta_disk_total_gb"][0].round(2)
            )
        ),
    }

    return runtime_dic
