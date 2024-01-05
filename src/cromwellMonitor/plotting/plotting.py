import datetime
import statistics
from math import isnan
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

import numpy as np
import pandas as pd
import seaborn as sns
from bokeh.io import curdoc
from bokeh.io import output_notebook, output_file, show
from bokeh.layouts import layout
from bokeh.models import ColumnDataSource, Div
from bokeh.models.widgets import DataTable, TableColumn
from bokeh.plotting import figure

from .data_processing import mean_of_string, get_1st_disk_usage, create_outlier_table

import logging

logger = logging.getLogger(__name__)


def generate_bar_plot(data_dict: dict, title: str, total_duration: int):
    """
    Generate a bar plot using bokeh
    @param data_dict:
    @param title:
    @param total_duration:
    @return:
    """

    # Ensure total duration is positive integer
    if total_duration < 0:
        logger.warning("Total duration is negative. Setting to 0.")
        total_duration = 0

    # Extract data from the dictionary
    categories = list(data_dict.keys())
    values = list(data_dict.values())

    # Create a Bokeh figure
    p = figure(
        x_range=categories,
        height=350,
        title=title,
        toolbar_location=None,
        tools="hover"
    )

    # Create bars using vbar with hover tooltips
    p.vbar(
        x=categories,
        top=values,
        width=0.5,
        color='blue',
        legend_label="Total Duration: {} seconds".format(total_duration),
        hover_fill_color='orange'
    )

    # Customize plot aesthetics
    curdoc().theme = 'caliber'
    p.xaxis.axis_label = 'Tasks'  # Set x-axis label
    p.yaxis.axis_label = 'Seconds'  # Set y-axis label
    p.xaxis.major_label_orientation = 1.2
    p.axis.axis_label_text_font_style = 'bold'  # Make axis labels bold
    p.axis.axis_label_text_font_size = '12pt'  # Set axis label font size
    p.axis.major_label_text_font_size = '10pt'  # Set tick label font size
    p.axis.axis_label_standoff = 15  # Set distance of labels from axis
    p.axis.visible = True
    p.ygrid.grid_line_color = '#D3D3D3'
    p.xgrid.grid_line_color = None
    p.hover.tooltips = [("Task", "@x"), ("Seconds", "@top")]  # Add hover tooltips

    return p


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

    # Place any outputs from bokeh into the following file
    output_file("{}_workflow_summary.html".format(parent_workflow_id))

    # Get Column names from pandas dataframe
    Columns = [TableColumn(field=Ci, title=Ci) for Ci in
               df_task_summary_named.columns]  # bokeh columns

    # Create bokeh datatables using pandas dataframe and column names
    task_summary_table = DataTable(columns=Columns, source=ColumnDataSource(
        df_task_summary_named))  # bokeh table

    # Create pie plot for workflow duration
    workflow_duration_pie = generate_bar_plot(data_dict=task_summary_duration,
                                              total_duration=workflow_duration,
                                              title="Workflow Duration in Seconds")

    # Divs
    workflow_title_div = Div(
        text="<h1>{} <br> Workflow Summary</h2>".format(parent_workflow_id), width=800)
    workflow_duration_div = Div(
        text="The workflow took {} seconds to complete.".format(workflow_duration),
        width=800)

    # Place the outputs from bokeh in this layout, which will be the layout in html file _workflow_summary.html
    show(layout([
        [workflow_title_div],
        [workflow_duration_div],
        [Div(text="<h3>Workflow Summary Table</h3>", width=800)],
        [task_summary_table],
        [workflow_duration_pie]
    ]))


def plot_bar(
        x_value: np.array, y_value: list, chart_title: str, y_label: str, x_label: str
):
    """
    Plot a bar plot
    @param x_value:
    @param y_value:
    @param chart_title:
    @param y_label:
    @param x_label:
    @return:
    """
    # Needed in every cell to show plots in notebook
    output_notebook()

    # what to display when hovering
    TOOLTIPS = [(x_label, ' @x'), (y_label, ' @top')]

    # plot attributes
    p = figure(x_range=x_value,
               height=350,
               sizing_mode="scale_width",
               title=chart_title,
               toolbar_location="left",
               tooltips=TOOLTIPS)

    # plot
    p.vbar(x=x_value, top=y_value, width=0.9)

    p.xgrid.grid_line_color = None
    p.y_range.start = 0

    # orient x labels vertically
    p.xaxis.major_label_orientation = "vertical"

    # axis titles
    p.xaxis.axis_label = x_label
    p.yaxis.axis_label = y_label

    # Add the Mean horizontal line
    p.ray(x=[0], y=[round(statistics.mean(y_value), 1)], length=len(x_value),
          color='red', angle=0,
          name="Mean: " + str(round(statistics.mean(y_value), 1)))

    # show(p)
    return p


def plot_sns_violin(
        x_value: np.array, y_value: list, chart_title: str, y_label: str, x_label: str
):
    """
    Plot a violin plot
    @param x_value:
    @param y_value:
    @param chart_title:
    @param y_label:
    @param x_label:
    @return:
    """

    output_notebook()
    # set plot size
    # plt.rcParams["figure.figsize"] = (7, 7)
    # plt.figure(figsize=(7,7))

    # create df using array of values
    df = pd.DataFrame(dict(Resource_Usage=y_value, Shard_Index=x_value))

    # plot styling
    sns.set(style="whitegrid")

    # plot values
    ax = sns.violinplot(y=df["Resource_Usage"]) #,figsize=(7, 7)

    # set plot lables
    ax.set(xlabel=x_label, ylabel=y_label, title=chart_title)

    # return(plt.show())
    return ax


def plot_1_metric(input_dataset: dict, title: str, y_label: str, x_label: str):
    """
    Plot a violin plot and bar plot for a given resource
    @param input_dataset:
    @param title:
    @param y_label:
    @param x_label:
    @return:
    """
    x = np.array((list(input_dataset.keys())))
    y = list(input_dataset.values())
    # o = get_outliers(shards=x, resource_value=y, resource_label=y_label)
    (upper_div, upper_table, lower_div, lower_table) = create_outlier_table(shards=x, resource_value=y, resource_label=y_label)
    violin_plot = plot_sns_violin(x, y, title, y_label, x_label)
    bar_plot = plot_bar(x, y, title, y_label, x_label)

    return upper_div, upper_table, lower_div, lower_table, violin_plot, bar_plot


def plot_shard_summary(
        parent_workflow_id: str, df_input: pd.DataFrame, task_name_input: str, pdf
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

    p_cpu_a = plot_1_metric(
        input_dataset=average_cpu_per_shard_sorted_dict,
        title="Average CPU Usage Per Shard",
        y_label="CPU Usage",
        x_label="Shards"
    )
    p_cpu_m = plot_1_metric(
        input_dataset=max_cpu_per_shard_sorted_dict,
        title="Max CPU Usage Per Shard",
        y_label="CPU Usage",
        x_label="Shards"
    )
    p_mem_m = plot_1_metric(
        input_dataset=max_memory_per_shard_sorted_dict,
        title="Max Memory Usage Per Shard",
        y_label="Memory Usage GB",
        x_label="Shards"
    )
    p_dis_m = plot_1_metric(
        input_dataset=max_disk_per_shard_sorted_dict,
        title="Max Disk Usage Per Shard",
        y_label="Disk Usage GB",
        x_label="Shards"
    )
    p_dur = plot_1_metric(
        input_dataset=duration_per_shard_sorted_dict,
        title="Time Duration Per Shard",
        y_label="Seconds",
        x_label="Shards"
    )

    Title_div = Div(
        text="<h1>{} {} Task Shard Summary</h2>".format(parent_workflow_id,
                                                        task_name_input),
        width=1200, height=25)

    # Writes to html file all the generated plots and tables for summary shard
    show(
        layout(
            [
                [Title_div],
                [Div(text="<h2>{} Average CPU Usage Per Shard</h2>".format(
                    task_name_input), width=600, height=25)],
                [p_cpu_a[0], p_cpu_a[2]],
                [p_cpu_a[1], p_cpu_a[3]],
                [p_cpu_a[5], Div(text="", width=50, height=25)],

                [Div(text="<h2>{} Max CPU Usage Per Shard</h2>".format(task_name_input),
                     width=600, height=25)],
                [p_cpu_m[0], p_cpu_m[2]],
                [p_cpu_m[1], p_cpu_m[3]],
                [p_cpu_m[5], Div(text="", width=50, height=25)],

                [Div(text="<h2>{} Max Memory Usage Per Shard</h2>".format(
                    task_name_input), width=600, height=25)],
                [p_mem_m[0], p_mem_m[2]],
                [p_mem_m[1], p_mem_m[3]],
                [p_mem_m[5], Div(text="", width=50, height=25)],

                [Div(text="<h2>{} Max Disk Usage Per Shard</h2>".format(
                    task_name_input), width=600, height=25)],
                [p_dis_m[0], p_dis_m[2]],
                [p_dis_m[1], p_dis_m[3]],
                [p_dis_m[5], Div(text="", width=50, height=25)],

                [Div(text="<h2>{} Time Duration Usage Per Shard</h2>".format(
                    task_name_input), width=600, height=25)],
                [p_dur[0], p_dur[2]],
                [p_dur[1], p_dur[3]],
                [p_dur[5]]
            ],
            sizing_mode='scale_width')
    )

    return plt

def get_shard_summary(
        df_monitoring: pd.DataFrame,
        task_name: str,
        parent_workflow_id: str,
        max_shards: int,
        pdf
):
    """
    Creates a pdf file with resource usage plots for each task name
    @param df_monitoring:
    @param task_name:
    @param parent_workflow_id:
    @param max_shards:
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
        pdf=pdf
    )

    resource_plt.subplots_adjust(hspace=0.5)
    pdf.savefig(bbox_inches='tight', pad_inches=0.5)
    plt.show()
    plt.close()


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
    cpu_plt.set_title("Task Name: " + task_name + " Shard: " + str(shard_number) + " Duration: " + str(task_shard_duration), fontsize=20)
    cpu_plt.plot(df_monitoring_task_shard.metrics_timestamp.astype('O'), cpu_used_percent_array, label='CPU Used')
    cpu_plt.plot([], [], ' ', label='Obtained CPU Cores: {}'.format(runtime_dic["available_cpu_cores"]))
    cpu_plt.plot([], [], ' ', label='Requested CPU Cores: {}'.format(runtime_dic["requested_cpu_cores"]))
    cpu_plt.legend(loc='upper center', bbox_to_anchor=(1.20, 0.8), shadow=True, ncol=1)
    cpu_plt.set_ylabel('CPU Percentage Used')
    cpu_plt.set_xlabel("Date Time")
    cpu_plt.set_xlim(df_monitoring_task_shard.metrics_timestamp.min(), df_monitoring_task_shard.metrics_timestamp.max())  # Set x-axis limits
    cpu_plt2 = cpu_plt.twiny()
    cpu_plt2.set_xlim(0, task_shard_duration)
    cpu_plt2.set_xlabel("Duration Time")
    cpu_plt.grid(True)

    mem_plt = plt.subplot(5, 1, 2)
    mem_plt.plot(df_monitoring_task_shard.metrics_timestamp.astype('O'), df_monitoring_task_shard.metrics_mem_used_gb, label='Memory Used')
    mem_plt.axhline(y=runtime_dic["available_mem_gb"], color='r', label='Max Memory GB: %.2f' % (runtime_dic["available_mem_gb"]))
    mem_plt.plot([], [], ' ', label='Obtained Memory GB: {}'.format(runtime_dic["available_mem_gb"]))
    mem_plt.plot([], [], ' ', label='Requested Memory GB: {}'.format(runtime_dic["requested_mem_gb"]))
    mem_plt.legend(loc='upper center', bbox_to_anchor=(1.20, 0.8), shadow=True, ncol=1)
    mem_plt.set_ylabel('Memory Used in GB')
    mem_plt.set_xlabel("Date Time")
    mem_plt.set_xlim(df_monitoring_task_shard.metrics_timestamp.min(), df_monitoring_task_shard.metrics_timestamp.max())  # Set x-axis limits
    mem_plt2 = mem_plt.twiny()
    mem_plt2.set_xlim(0, task_shard_duration)
    mem_plt2.set_xlabel("Duration Time")
    mem_plt.grid(True)

    disk_plt = plt.subplot(5, 1, 3)
    disk_plt.plot(df_monitoring_task_shard.metrics_timestamp.astype('O'), disk_used_gb_array, label='Disk Used')
    disk_plt.axhline(y=runtime_dic["available_disk_gb"], color='r', label='Max Disksize GB: %.2f' % (runtime_dic["available_disk_gb"]))
    disk_plt.plot([], [], ' ', label='Obtained Disksize GB: {}'.format(runtime_dic["available_disk_gb"]))
    disk_plt.plot([], [], ' ', label='Requested Disksize GB: {}'.format(runtime_dic["requested_disk_gb"]))
    disk_plt.legend(loc='upper center', bbox_to_anchor=(1.20, 0.8), shadow=True, ncol=1)
    disk_plt.set_ylabel('Diskspace Used in GB')
    disk_plt.set_xlabel("Date Time")
    disk_plt.set_xlim(df_monitoring_task_shard.metrics_timestamp.min(), df_monitoring_task_shard.metrics_timestamp.max())  # Set x-axis limits
    disk_plt2 = disk_plt.twiny()
    disk_plt2.set_xlim(0, task_shard_duration)
    disk_plt2.set_xlabel("Duration Time")
    disk_plt.grid(True)

    disk_read_plt = plt.subplot(5, 1, 4)
    disk_read_plt.plot(df_monitoring_task_shard.metrics_timestamp.astype('O'), disk_read_iops_array, label='Disk Read IOps')
    disk_read_plt.set_ylabel('Disk Read IOps')
    disk_read_plt.set_xlabel("Date Time")
    disk_read_plt.set_xlim(df_monitoring_task_shard.metrics_timestamp.min(), df_monitoring_task_shard.metrics_timestamp.max())  # Set x-axis limits
    disk_read_plt2 = disk_read_plt.twiny()
    disk_read_plt2.set_xlim(0, task_shard_duration)
    disk_read_plt2.set_xlabel("Duration Time")
    disk_read_plt.grid(True)

    disk_write_plt = plt.subplot(5, 1, 5)
    disk_write_plt.plot(df_monitoring_task_shard.metrics_timestamp.astype('O'), disk_write_iops_array, label='Disk Write_IOps')
    disk_write_plt.set_ylabel('Disk Write_IOps')
    disk_write_plt.set_xlabel("Date Time")
    disk_write_plt.set_xlim(df_monitoring_task_shard.metrics_timestamp.min(), df_monitoring_task_shard.metrics_timestamp.max())  # Set x-axis limits
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
        task_shard_duration = round(datetime.timedelta.total_seconds(max_datetime - min_datetime))

        # create an array for to hold the y values from the list columns
        cpu_used_percent_array = [np.asarray(x).mean() for x in df_monitoring_task_shard.metrics_cpu_used_percent]
        disk_used_gb_array = [np.asarray(x).max() for x in df_monitoring_task_shard.metrics_disk_used_gb]
        disk_read_iops_array = [np.asarray(x).max() for x in df_monitoring_task_shard.metrics_disk_read_iops]
        disk_write_iops_array = [np.asarray(x).max() for x in df_monitoring_task_shard.metrics_disk_write_iops]

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


def plot_resource_usage(df_monitoring, parent_workflow_id, task_names):
    """
    Creates a pdf file with resource usage plots for each task name
    @param df_monitoring:
    @param parent_workflow_id:
    @param task_names:
    @return:
    """

    with PdfPages(parent_workflow_id + '_resource_monitoring.pdf') as pdf:

        for task_name in task_names:
            # Gets the all shards for a given task name
            shards = df_monitoring.metadata_runtime.runtime_shard.loc[
                (df_monitoring.metadata_runtime['runtime_task_call_name'] == task_name)]
            shards = shards.sort_values().unique()

            # If shard counts is greater than 10 then gets 10 longest running shards for a given task name
            max_shards = 2
            if len(shards) >= max_shards:
                get_shard_summary(
                    df_monitoring=df_monitoring,
                    task_name=task_name,
                    parent_workflow_id=parent_workflow_id,
                    max_shards=max_shards,
                    pdf=pdf
                )

            # Create detailed resource usage plots
            if len(shards) < max_shards:
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
        "requested_disk_gb": df_monitoring_metadata_runtime_task_shard.iloc[0].at[
            'meta_disk_total_gb']
    }
    return runtime_dic
