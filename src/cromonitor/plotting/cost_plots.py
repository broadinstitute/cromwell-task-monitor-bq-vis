# Class for plotting cost data.
from typing import Optional, Union

import pandas as pd
import plotly.express as px
from plotly.graph_objs._figure import Figure

from ..logging import logging as log

COST_COLUMN_NAME = "cost"
COST_DESCRIPTION_COLUMN_NAME = "cost_description"
TASK_NAME_COLUMN_NAME = "task_name"


class CostPlots:
    """
    Class for plotting cost data.
    """

    def __init__(self, cost_data: pd.DataFrame):
        self.cost_data: pd.DataFrame = cost_data
        self.cost_data.fillna("NA", inplace=True)  # Fill NaN values with 'NA'

    def plot_workflow_cost(
        self,
        title: Optional[str] = "Workflow Cost Per Task",
        group_by_description: Optional[bool] = False,
        color: Optional[str] = COST_DESCRIPTION_COLUMN_NAME,
        threshold_cost_percent: Optional[float] = None,
    ) -> Figure:
        """
        Plot the cost data
        :param title: Title for the plot
        :param group_by_description: Group the cost by description
        :param color: Color the bars by cost description
        or specify a column to color by. If set to None, no coloring will be done.
        :param threshold_cost_percent: Plots the greatest costing tasks that contribute
        to the specified percentage of the total cost. (0 plots the most expensive
        task, 100 plots all tasks)
        :return: A plotly Figure object with the cost data plot.
        """

        return plotly_bar_workflow_cost(
            self.cost_data,
            title=title,
            group_des=group_by_description,
            color=color,
            threshold_cost_percent=threshold_cost_percent,
            x_axis_title="Task Name",
        )

    def plot_task_cost(
        self,
        task_name: str,
        title: Optional[str] = None,
        color: Optional[str] = COST_DESCRIPTION_COLUMN_NAME,
        threshold_cost_percent: Optional[float] = None,
        task_name_column=TASK_NAME_COLUMN_NAME,
    ) -> Figure:
        """
        Plot the cost data

        :param task_name: Task name to plot
        :param title: Title for the plot
        :param color: Color the bars by cost description
        or specify a column to color by.
        If set to None, no coloring will be done.
        :param threshold_cost_percent: Plots the greatest costing item that contributes
        to the specified percentage of the total cost. (0 plots the most expensive
        task, 100 plots all tasks)
        :param task_name_column: Name of the column containing the task names
        :return: A plotly Figure object with the cost data plot.
        """
        # Check if task_name is in the cost_data
        if task_name not in self.cost_data[task_name_column].unique():
            log.handle_user_error(
                err=None, message=f"Task name {task_name} not found in the cost data."
            )
            raise ValueError(f"Task name {task_name} not found in the cost data.")

        if title is None:
            title = f"Cost for {task_name}"

        task_cost_data = self.cost_data[self.cost_data[task_name_column] == task_name]

        return plotly_bar_task_cost(
            cost_df=task_cost_data,
            title=title,
            color=color,
            threshold_cost_percent=threshold_cost_percent,
            x_axis_title="Cost Description",
        )


def plotly_bar_workflow_cost(
    cost_df,
    title,
    x_axis_title: str,
    group_des: Optional[bool] = False,
    color: Optional[str] = None,
    threshold_cost_percent: Optional[float] = None,
    task_name_column=TASK_NAME_COLUMN_NAME,
    cost_column=COST_COLUMN_NAME,
    y_axis_title: str = "Cost",
    legend_title: str = "Cost Description",
) -> Figure:
    """
    Plot the cost data



    :param cost_df: DataFrame with cost data
    :param title: Title for the plot
    :param x_axis_title: Title for the x-axis
    :param group_des: Group the cost by description
    :param color: Color the bars by specified column
    :param threshold_cost_percent: Plots the greatest costing tasks that contribute
    to the specified percentage of the total cost. (0 plots the most expensive
    task, 100 plots all tasks)
    :param task_name_column: Name of the column containing the task names
    :param cost_column: Name of the column containing the cost data
    :param legend_title: Title for the legend
    :param y_axis_title: Title for the y-axis
    :return: A plotly Figure object with the cost data plot.
    """

    cost_df_sorted = sort_dataframe_by_cost(
        cost_df=cost_df,
        grouping_column=task_name_column,
        threshold_cost_percent=threshold_cost_percent,
    )

    x_axis_order = get_sorted_group_order(
        cost_df_sorted=cost_df_sorted,
        cost_description_column=task_name_column,
        cost_column=cost_column
    )

    fig = px.bar(
        cost_df_sorted,
        x=task_name_column,
        y=cost_column,
        color=color,
        title=title,
        hover_data="machine_spec",
        category_orders={task_name_column: x_axis_order},
    )

    fig.update_layout(
        xaxis_title=x_axis_title,
        yaxis_title=y_axis_title,
        legend_title=legend_title,
    )
    if group_des:
        fig.update_layout(
            barmode="group",
            bargap=0.15,  # gap between bars of adjacent location coordinates.
            bargroupgap=0.1,  # gap between bars of the same location coordinate.
        )

    return fig


def plotly_bar_task_cost(
    cost_df,
    title,
    x_axis_title: str,
    color: Optional[str] = None,
    threshold_cost_percent: Optional[float] = None,
    cost_column=COST_COLUMN_NAME,
    cost_description_column=COST_DESCRIPTION_COLUMN_NAME,
    y_axis_title: str = "Cost",
    legend_title: str = "Cost Description"
) -> Figure:
    """
    Plot the cost data

    :param x_axis_title:
    :param cost_df: DataFrame with cost data
    :param title: Title for the plot
    :param color: Color the bars by specified column
    :param threshold_cost_percent: Optional (float between 0 and 100).
    :param cost_description_column: Name of the column containing the cost descriptions
    :param cost_column: Name of the column containing the cost data
    :param legend_title: Title for the legend
    :param y_axis_title: Title for the y-axis
    :return: A plotly Figure object with the cost data plot.
    """

    cost_df_sorted = sort_dataframe_by_cost(
        cost_df=cost_df,
        grouping_column=cost_description_column,
        threshold_cost_percent=threshold_cost_percent,
    )

    x_axis_order = get_sorted_group_order(
        cost_df_sorted=cost_df_sorted,
        cost_description_column=cost_description_column,
        cost_column=cost_column,
    )

    fig = px.bar(
        cost_df_sorted,
        x=cost_description_column,
        y=cost_column,
        color=color,
        title=title,
        hover_data="machine_spec",
        category_orders={cost_description_column: x_axis_order},
    )

    fig.update_layout(
        xaxis_title=x_axis_title,
        yaxis_title=y_axis_title,
        legend_title=legend_title,
    )

    return fig


def sort_dataframe_by_cost(
        cost_df: pd.DataFrame,
        grouping_column: str,
        threshold_cost_percent=None,
        cost_column: str = COST_COLUMN_NAME,
) -> pd.DataFrame:
    """
    Sort the DataFrame by cost in descending order.
    :param cost_df: DataFrame to sort
    :param threshold_cost_percent: Percentage of the total cost to consider
    :param grouping_column: Column to group the rows by
    :param cost_column: Column to sort by
    :return: Returns the sorted DataFrame
    """
    if threshold_cost_percent:
        cost_df_sorted = filter_dataframe_by_cost_threshold(
            dataframe=cost_df,
            threshold_percent=threshold_cost_percent,
            grouping_column=grouping_column,
        )
        # Sort the DataFrame by 'cost' in descending order
        cost_df_sorted = cost_df_sorted.sort_values(by=[cost_column], ascending=False)
    else:
        # Sort the DataFrame by 'cost' in descending order
        cost_df_sorted = cost_df.sort_values(by=[cost_column], ascending=False)

    return cost_df_sorted


def filter_dataframe_by_cost_threshold(
    dataframe: pd.DataFrame,
    threshold_percent: float,
    grouping_column=TASK_NAME_COLUMN_NAME,
    cost_column=COST_COLUMN_NAME,
) -> pd.DataFrame:
    """
    Apply a threshold to the DataFrame based on the total cost of the tasks.
    :param dataframe: Dataframe to apply a threshold to
    :param threshold_percent: threshold percentage to apply
    :param grouping_column: The column used to group the rows
    :param cost_column: The cost column names
    :return: The filtered DataFrame
    """

    if not 0 <= threshold_percent <= 100:
        log.handle_user_error(
            err=None, message="Threshold percentage must be between 0 and 100."
        )

    grouped_and_sorted_df = (
        dataframe.groupby(grouping_column)[cost_column]
        .sum()
        .sort_values(ascending=False)
    )

    total_cost = dataframe[cost_column].sum()
    cost_threshold = total_cost * (threshold_percent / 100)

    filtered_dataframe = pd.DataFrame()
    for (
        group_index,
        cost,
    ) in (
        grouped_and_sorted_df.items()
    ):  # Iterate over the task names starting from the most expensive
        # Filter the dataframe for the current task
        grouped_df = dataframe[dataframe[grouping_column] == group_index]

        # If the filtered_dataframe is empty, this is the first iteration
        # So; we add the current grouper's dataframe to the filtered_dataframe
        if filtered_dataframe.empty:
            filtered_dataframe = pd.concat([filtered_dataframe, grouped_df])
        else:
            # If the filtered_dataframe is not empty, we check if the total cost of
            # the current filtered_dataframe and the current grouped dataframe is less
            # than or equal to the cost threshold If it is, we add the current grouped
            # dataframe to the filtered_dataframe
            if (
                pd.concat([filtered_dataframe, grouped_df])[cost_column].sum()
                <= cost_threshold
            ):
                filtered_dataframe = pd.concat([filtered_dataframe, grouped_df])
            else:
                # If the total cost is greater than the cost threshold, we break the
                # loop; this means we stop adding more grouped rows to the
                # filtered_dataframe
                break

    return filtered_dataframe


def get_sorted_group_order(cost_df_sorted, cost_description_column, cost_column):
    """
    Group by 'task_name', calculate the sum of 'cost' for each task, sort in
    descending order. Used to order the x-axis based on the total cost of each task.
    :param cost_df_sorted: DataFrame with cost data
    :param cost_description_column: Name of the column containing the cost descriptions
    :param cost_column: Name of the column containing the cost data
    :return: Index of the sorted DataFrame
    """
    return (
        cost_df_sorted.groupby(cost_description_column)[cost_column]
        .sum()
        .sort_values(ascending=False)
        .index
    )