import datetime
from math import isnan
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

import numpy as np
import pandas as pd
import seaborn as sns
from bokeh.io import output_file

import plotly.io as pio
from plotly.subplots import make_subplots

import plotly.offline as pyo
import plotly.graph_objects as go

from .data_processing import mean_of_string, get_1st_disk_usage, \
    create_outlier_table_plotly

import logging

logger = logging.getLogger(__name__)


def remove_nan(input_data: dict):
    """
    Remove nan values from a dictionary
    @param input_data:
    @return:
    """
    clean_dict = {k: input_data[k] for k in input_data if not isnan(input_data[k])}
    return clean_dict


def calculate_workflow_duration(df_monitoring) -> int:
    """
    Calculate the total duration of a workflow in seconds.

    Parameters:
    df_monitoring (DataFrame): The dataframe containing the monitoring metrics.

    Returns:
    int: The total duration of the workflow in seconds.
    """
    latest_end_datetime = max(df_monitoring.metrics['metrics_timestamp'])
    earliest_start_datetime = min(df_monitoring.metrics['metrics_timestamp'])
    workflow_duration: int = round(
        datetime.timedelta.total_seconds(latest_end_datetime - earliest_start_datetime))

    return workflow_duration


def generate_workflow_summary(
        parent_workflow_id: str,
        df_task_summary_named: pd.DataFrame,
        task_summary_duration: dict,
        df_monitoring: pd.DataFrame,
        make_pdf: bool = False
):
    """
    Generate a workflow summary html file using bokeh
    @param parent_workflow_id:
    @param df_task_summary_named:
    @param task_summary_duration:
    @param df_monitoring:
    @return:
    """

    workflow_duration = calculate_workflow_duration(df_monitoring=df_monitoring)



    # Create table using plotly
    task_summary_table = go.Table(
        header=dict(values=list(df_task_summary_named.columns),
                    fill_color='paleturquoise',
                    align='left'),
        cells=dict(values=[df_task_summary_named[col] for col in df_task_summary_named.columns],
                   fill_color='lavender',
                   align='left'))

    # Ensure total duration is positive integer
    if workflow_duration < 0:
        logger.warning(f"Total duration is negative.")

    workflow_duration_bar = plot_bar_plotly_using_dict(
        data_dict=task_summary_duration,
        x_legend_label="Tasks",
        y_legend_label="Seconds",
        legend_title=f'Total Workflow Duration: {workflow_duration} seconds',
        show_legend=True,
    )

    fig = make_subplots(
        rows=2,
        cols=1,
        vertical_spacing=0.1,
        specs=[
            [{"type": "table"}],
            [{"type": "bar"}]
        ],
        subplot_titles=[
            "Workflow Summary Table",
            "Workflow Duration in Seconds"
        ]
    )

    # Add plots to the subplots
    fig.add_trace(task_summary_table, row=1, col=1)
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
    if make_pdf:
        pio.write_image(
            fig, "{}_workflow_summary.pdf".format(parent_workflow_id), format='pdf'
        )

    return fig


def plot_bar_plotly(
        x_value: list, y_value: list, y_label: str, x_label: str
):
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
        legend_title: str =None,
):
    """
    Generate a bar plot using plotly
    @param data_dict:
    @param x_legend_label:
    @param y_legend_label:
    @param show_legend:
    @param legend_title:
    @return:
    """

    # Extract data from the dictionary
    x_values = list(data_dict.keys())
    y_values = list(data_dict.values())

    # Create bars with hover tooltips
    return go.Bar(
        x=x_values,
        y=y_values,
        hovertemplate=f'{x_legend_label}: %{{x}}<br>{y_legend_label}: %{{y:,.0f}}<br>',
        name=legend_title,
        showlegend=show_legend
    )

def create_violin_plot_plotly(y_values: list, x_values: list , y_label: str, x_label: str):
    """
    Create a violin plot with outliers marked using Plotly
    @param title:
    @param data:
    @param y_label:
    @param x_label:
    @return:
    """
    # Create a DataFrame from the data
    df = pd.DataFrame({"y_value": y_values, "x_value": x_values})

    return go.Violin(
        y=df['y_value'],
        box_visible=True,
        line_color='black',
        meanline_visible=True,
        fillcolor='blue',
        opacity=0.6,
        points='outliers',
        hoverinfo='y',
        hovertemplate=
        f'<i>{x_label}</i>: %{{text}}<br>' +
        f'<i>{y_label}</i>: %{{y}}',
        text=df['x_value'].values,

    )


def generate_resource_plots_and_outliers(
        input_dataset: dict, y_label: str, x_label: str
):
    """
    Generate an outlier table, plot a violin plot and bar plot for a given resource
    @param input_dataset:
    @param title:
    @param y_label:
    @param x_label:
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


def plot_shard_summary(
        parent_workflow_id: str, df_input: pd.DataFrame, task_name_input: str,
        plt_height: int = None, plt_width: int = None
):
    # Removes shards with null in columns being measured.
    df_droped_na = df_input.metrics_runtime.dropna(
        subset=['meta_duration_sec', 'metrics_disk_used_gb', 'metrics_mem_used_gb',
                'metrics_mem_used_gb', 'metrics_cpu_used_percent'])

    # put dataframe into and array
    summary_shards = df_droped_na.runtime_shard.loc[
        (df_droped_na['runtime_task_call_name'] == task_name_input)].unique()

    # For each element in sereas get the average cpu, max cpu, max mem, max disk
    # usage from monitoring datafram put in a dict
    average_cpu_per_shard_dict = {}
    max_cpu_per_shard_dict = {}
    max_memory_per_shard_dict = {}
    max_disk_per_shard_dict = {}
    duration_per_shard_dict = {}
    # for shard in summary_shards[0:99]: #used to test code on 100 shards
    for shard in summary_shards:
        # create dataframe for a given task name and shard
        df_summary_shard = df_input.metrics_runtime.loc[
            (df_input.metrics_runtime['runtime_task_call_name'] == task_name_input) & (
                    df_input.metrics_runtime['runtime_shard'] == shard)]

        cpu_time_mean = df_summary_shard.metrics_cpu_used_percent.apply(mean_of_string)

        average_cpu_per_shard_dict[str(shard)] = cpu_time_mean.mean()
        max_cpu_per_shard_dict[str(shard)] = cpu_time_mean.max()

        max_memory_per_shard_dict[
            str(shard)] = df_summary_shard.metrics_mem_used_gb.max()

        max_disk_per_shard_dict[
            str(shard)] = df_summary_shard.metrics_disk_used_gb.apply(
            get_1st_disk_usage).max()

        duration_per_shard_dict[str(shard)] = \
            df_summary_shard['meta_duration_sec'].iloc[0]

    # Removes nan values from dict
    average_cpu_per_shard_clean_dict = remove_nan(average_cpu_per_shard_dict)
    max_cpu_per_shard_clean_dict = remove_nan(max_cpu_per_shard_dict)
    max_memory_per_shard_clean_dict = remove_nan(max_memory_per_shard_dict)
    max_disk_per_shard_clean_dict = remove_nan(max_disk_per_shard_dict)
    duration_per_shard_clean_dict = remove_nan(duration_per_shard_dict)

    # Sort resource dict by value
    average_cpu_per_shard_sorted_dict = {k: v for k, v in sorted(
        average_cpu_per_shard_clean_dict.items(), reverse=True,
        key=lambda item: item[1])}
    max_cpu_per_shard_sorted_dict = {k: v for k, v in
                                     sorted(max_cpu_per_shard_clean_dict.items(),
                                            reverse=True, key=lambda item: item[1])}
    max_memory_per_shard_sorted_dict = {k: v for k, v in
                                        sorted(max_memory_per_shard_clean_dict.items(),
                                               reverse=True, key=lambda item: item[1])}
    max_disk_per_shard_sorted_dict = {k: v for k, v in
                                      sorted(max_disk_per_shard_clean_dict.items(),
                                             reverse=True, key=lambda item: item[1])}
    duration_per_shard_sorted_dict = {k: v for k, v in
                                      sorted(duration_per_shard_clean_dict.items(),
                                             reverse=True, key=lambda item: item[1])}

    output_file("{}_{}_shard_summary.html".format(parent_workflow_id, task_name_input))

    p_cpu_a = generate_resource_plots_and_outliers(
        input_dataset=average_cpu_per_shard_sorted_dict,
        y_label="CPU Usage",
        x_label="Shards"
    )
    p_cpu_m = generate_resource_plots_and_outliers(
        input_dataset=max_cpu_per_shard_sorted_dict,
        y_label="CPU Usage",
        x_label="Shards"
    )
    p_mem_m = generate_resource_plots_and_outliers(
        input_dataset=max_memory_per_shard_sorted_dict,
        y_label="Memory Usage GB",
        x_label="Shards"
    )
    p_dis_m = generate_resource_plots_and_outliers(
        input_dataset=max_disk_per_shard_sorted_dict,
        y_label="Disk Usage GB",
        x_label="Shards"
    )
    p_dur = generate_resource_plots_and_outliers(
        input_dataset=duration_per_shard_sorted_dict,
        y_label="Seconds",
        x_label="Shards"
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
            "Time Duration"
            "Time Duration Lower Outliers",
            "Time Duration Upper Outliers",
        ]

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
            parent_workflow_id,
            task_name_input
        ),
        showlegend=False,
    )

    return fig


def get_shard_summary(
        df_monitoring: pd.DataFrame,
        task_name: str,
        parent_workflow_id: str,
        max_shards: int,
        plt_height: int = 5000,
        plt_width: int = 1200,
):
    """
    Creates a pdf file with resource usage plots for each task name
    @param df_monitoring:
    @param task_name:
    @param parent_workflow_id:
    @param max_shards:
    @param plt_width:
    @param plt_height:
    @return:
    """
    # create and sort meta table by duration
    df_monitoring_task = df_monitoring.metadata_runtime.loc[
        df_monitoring.metadata_runtime['meta_task_call_name'] == task_name
        ]

    # removes duplicate shards
    df_monitoring_task_uniqueShard = df_monitoring_task.drop_duplicates(
        subset="meta_shard")
    df_monitoring_task_sorted_duration = df_monitoring_task_uniqueShard.sort_values(
        by='meta_duration_sec', ascending=False
    )

    # replace all shards in variable shards with the first 50 of the sorted duration table
    shards = df_monitoring_task_sorted_duration.meta_shard.head(max_shards)

    # Get shard summary
    resource_plt = plot_shard_summary(
        df_input=df_monitoring,
        task_name_input=task_name,
        parent_workflow_id=parent_workflow_id,
        plt_height=plt_height,
        plt_width=plt_width
    )

    return resource_plt


def plot_detailed_resource_usage(
        task_name: str,
        shard_number: int,
        task_shard_duration: int,
        df_monitoring_task_shard: pd.DataFrame,
        cpu_used_percent_array: list,
        runtime_dic: dict,
        disk_used_gb_array: list,
        disk_read_iops_array: list,
        disk_write_iops_array: list
) -> plt:
    """
    Creates a plot with resource usage figure for a given task and its shard
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
    plt.rcParams["figure.figsize"] = (15, 20)
    sns.set(style="whitegrid")

    cpu_plt = plt.subplot(5, 1, 1)
    cpu_plt.set_title("Task Name: " + task_name + " Shard: " + str(
        shard_number) + " Duration: " + str(task_shard_duration), fontsize=20)
    cpu_plt.plot(df_monitoring_task_shard.metrics_timestamp.astype('O'),
                 cpu_used_percent_array, label='CPU Used')
    cpu_plt.plot([], [], ' ', label='Obtained CPU Cores: {}'.format(
        runtime_dic["available_cpu_cores"]))
    cpu_plt.plot([], [], ' ', label='Requested CPU Cores: {}'.format(
        runtime_dic["requested_cpu_cores"]))
    cpu_plt.legend(loc='upper center', bbox_to_anchor=(1.20, 0.8), shadow=True, ncol=1)
    cpu_plt.set_ylabel('CPU Percentage Used')
    cpu_plt.set_xlabel("Date Time")
    cpu_plt.set_xlim(df_monitoring_task_shard.metrics_timestamp.min(),
                     df_monitoring_task_shard.metrics_timestamp.max())  # Set x-axis limits
    cpu_plt2 = cpu_plt.twiny()
    cpu_plt2.set_xlim(0, task_shard_duration)
    cpu_plt2.set_xlabel("Duration Time")
    cpu_plt.grid(True)

    mem_plt = plt.subplot(5, 1, 2)
    mem_plt.plot(df_monitoring_task_shard.metrics_timestamp.astype('O'),
                 df_monitoring_task_shard.metrics_mem_used_gb, label='Memory Used')
    mem_plt.axhline(y=runtime_dic["available_mem_gb"], color='r',
                    label='Max Memory GB: %.2f' % (runtime_dic["available_mem_gb"]))
    mem_plt.plot([], [], ' ',
                 label='Obtained Memory GB: {}'.format(runtime_dic["available_mem_gb"]))
    mem_plt.plot([], [], ' ', label='Requested Memory GB: {}'.format(
        runtime_dic["requested_mem_gb"]))
    mem_plt.legend(loc='upper center', bbox_to_anchor=(1.20, 0.8), shadow=True, ncol=1)
    mem_plt.set_ylabel('Memory Used in GB')
    mem_plt.set_xlabel("Date Time")
    mem_plt.set_xlim(df_monitoring_task_shard.metrics_timestamp.min(),
                     df_monitoring_task_shard.metrics_timestamp.max())  # Set x-axis limits
    mem_plt2 = mem_plt.twiny()
    mem_plt2.set_xlim(0, task_shard_duration)
    mem_plt2.set_xlabel("Duration Time")
    mem_plt.grid(True)

    disk_plt = plt.subplot(5, 1, 3)
    disk_plt.plot(df_monitoring_task_shard.metrics_timestamp.astype('O'),
                  disk_used_gb_array, label='Disk Used')
    disk_plt.axhline(y=runtime_dic["available_disk_gb"], color='r',
                     label='Max Disksize GB: %.2f' % (runtime_dic["available_disk_gb"]))
    disk_plt.plot([], [], ' ', label='Obtained Disksize GB: {}'.format(
        runtime_dic["available_disk_gb"]))
    disk_plt.plot([], [], ' ', label='Requested Disksize GB: {}'.format(
        runtime_dic["requested_disk_gb"]))
    disk_plt.legend(loc='upper center', bbox_to_anchor=(1.20, 0.8), shadow=True, ncol=1)
    disk_plt.set_ylabel('Diskspace Used in GB')
    disk_plt.set_xlabel("Date Time")
    disk_plt.set_xlim(df_monitoring_task_shard.metrics_timestamp.min(),
                      df_monitoring_task_shard.metrics_timestamp.max())  # Set x-axis limits
    disk_plt2 = disk_plt.twiny()
    disk_plt2.set_xlim(0, task_shard_duration)
    disk_plt2.set_xlabel("Duration Time")
    disk_plt.grid(True)

    disk_read_plt = plt.subplot(5, 1, 4)
    disk_read_plt.plot(df_monitoring_task_shard.metrics_timestamp.astype('O'),
                       disk_read_iops_array, label='Disk Read IOps')
    disk_read_plt.set_ylabel('Disk Read IOps')
    disk_read_plt.set_xlabel("Date Time")
    disk_read_plt.set_xlim(df_monitoring_task_shard.metrics_timestamp.min(),
                           df_monitoring_task_shard.metrics_timestamp.max())  # Set x-axis limits
    disk_read_plt2 = disk_read_plt.twiny()
    disk_read_plt2.set_xlim(0, task_shard_duration)
    disk_read_plt2.set_xlabel("Duration Time")
    disk_read_plt.grid(True)

    disk_write_plt = plt.subplot(5, 1, 5)
    disk_write_plt.plot(df_monitoring_task_shard.metrics_timestamp.astype('O'),
                        disk_write_iops_array, label='Disk Write_IOps')
    disk_write_plt.set_ylabel('Disk Write_IOps')
    disk_write_plt.set_xlabel("Date Time")
    disk_write_plt.set_xlim(df_monitoring_task_shard.metrics_timestamp.min(),
                            df_monitoring_task_shard.metrics_timestamp.max())  # Set x-axis limits
    disk_write_plt2 = disk_write_plt.twiny()
    disk_write_plt2.set_xlim(0, task_shard_duration)
    disk_write_plt2.set_xlabel("Duration Time")
    disk_write_plt.grid(True)

    return plt


def plot_shards(
        df_monitoring: pd.DataFrame, task_name: str, shards, pdf):
    for shard in shards:
        df_monitoring_task_shard = df_monitoring.metrics_runtime.loc[
            (df_monitoring.metrics_runtime['runtime_task_call_name'] == task_name) &
            (df_monitoring.metrics_runtime['runtime_shard'] == shard)
            ]
        df_monitoring_metadata_runtime_task_shard = df_monitoring.metadata_runtime.loc[
            (df_monitoring.metadata_runtime['runtime_task_call_name'] == task_name) &
            (df_monitoring.metadata_runtime['runtime_shard'] == shard)
            ]
        df_monitoring_task_shard = df_monitoring_task_shard.sort_values(
            by='metrics_timestamp'
        )

        # Calculate the duration of the task shard
        max_datetime = max(df_monitoring_task_shard['metrics_timestamp'])
        min_datetime = min(df_monitoring_task_shard['metrics_timestamp'])
        task_shard_duration = round(
            datetime.timedelta.total_seconds(max_datetime - min_datetime))

        # create an array for to hold the y values from the list columns
        cpu_used_percent_array = [np.asarray(x).mean() for x in
                                  df_monitoring_task_shard.metrics_cpu_used_percent]
        disk_used_gb_array = [np.asarray(x).max() for x in
                              df_monitoring_task_shard.metrics_disk_used_gb]
        disk_read_iops_array = [np.asarray(x).max() for x in
                                df_monitoring_task_shard.metrics_disk_read_iops]
        disk_write_iops_array = [np.asarray(x).max() for x in
                                 df_monitoring_task_shard.metrics_disk_write_iops]

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
            disk_write_iops_array=disk_write_iops_array
        )

        resource_plt.subplots_adjust(hspace=0.5)
        pdf.savefig(bbox_inches='tight', pad_inches=0.5)
        plt.show()
        plt.close()
    return plt


def plot_resource_usage(df_monitoring, parent_workflow_id, task_names, plt_height=2400,
                        plt_width=600):
    """
    Creates a pdf file with resource usage plots for each task name
    @param df_monitoring:
    @param parent_workflow_id:
    @param task_names:
    @return:
    """

    pdf_file_name = parent_workflow_id + '_resource_monitoring.pdf'

    for task_name in task_names:
        # Gets the all shards for a given task name
        shards = df_monitoring.metadata_runtime.runtime_shard.loc[
            (df_monitoring.metadata_runtime['runtime_task_call_name'] == task_name)]
        shards = shards.sort_values().unique()

        # If shard counts is greater than 10 then gets 10 longest running shards for a given task name
        max_shards = 2
        if len(shards) >= max_shards:
            shard_sum_fig = get_shard_summary(
                df_monitoring=df_monitoring,
                task_name=task_name,
                parent_workflow_id=parent_workflow_id,
                max_shards=max_shards,
                plt_height=plt_height,
                plt_width=plt_width,
            )
            pio.write_image(shard_sum_fig, pdf_file_name, format='pdf')
            shard_sum_fig.show()

        # Create detailed resource usage plots
        if len(shards) < max_shards:
            with PdfPages(pdf_file_name) as pdf:
                plot_shards(
                    df_monitoring=df_monitoring,
                    task_name=task_name,
                    shards=shards,
                    pdf=pdf
                )


def create_runtime_dict(
        df_monitoring_metadata_runtime_task_shard: pd.DataFrame
) -> dict:
    """
    Creates a dictionary of runtime attributes
    @param df_monitoring_metadata_runtime_task_shard:
    @return:
    """
    runtime_dic = {
        "available_cpu_cores": df_monitoring_metadata_runtime_task_shard.iloc[0].at[
            'runtime_cpu_count'],
        "available_mem_gb": df_monitoring_metadata_runtime_task_shard.iloc[0].at[
            'runtime_mem_total_gb'].round(2),
        "available_disk_gb": df_monitoring_metadata_runtime_task_shard.iloc[0].at[
            'runtime_disk_total_gb'][0].round(2),
        "requested_cpu_cores": df_monitoring_metadata_runtime_task_shard.iloc[0].at[
            'meta_cpu'],
        "requested_mem_gb": df_monitoring_metadata_runtime_task_shard.iloc[0].at[
            'meta_mem_total_gb'],
        # meta_disk_total_gb is a list of disk sizes even if there is only one disk
        "requested_disk_gb": df_monitoring_metadata_runtime_task_shard.iloc[0].at[
            'meta_disk_total_gb'][0].round(2)
    }
    return runtime_dic
