# Class for plotting cost data.
from typing import Optional, Union

import pandas as pd
import plotly.express as px

from ..logging import logging as log


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
        color: Optional[str] = "cost_description",
    ) -> px.bar:
        """
        Plot the cost data
        :param title: Title for the plot
        :param group_by_description: Group the cost by description
        :param color: Color the bars by cost description
        or specify a column to color by.
        If set to None, no coloring will be done.
        :return: A plotly Figure object with the cost data plot.
        """

        return plotly_bar_cost(
            self.cost_data, title=title, group_des=group_by_description, color=color
        )

    def plot_task_cost(
        self,
        task_name: str,
        title: Optional[str] = "Task Cost Per Workflow",
        group_by_description: Optional[bool] = False,
        color: Optional[str] = "cost_description",
    ) -> Union[px.bar, None]:
        """
        Plot the cost data
        :param task_name: Task name to plot
        :param title: Title for the plot
        :param group_by_description: Group the cost by description
        :param color: Color the bars by cost description
        or specify a column to color by.
        If set to None, no coloring will be done.
        :return: A plotly Figure object with the cost data plot.
        """
        # Check if task_name is in the cost_data
        if task_name not in self.cost_data["task_name"].unique():
            log.handle_user_error(
                err=None, message=f"Task name {task_name} not found in the cost data."
            )
            return None

        task_cost_data = self.cost_data[self.cost_data["task_name"] == task_name]

        return plotly_bar_cost(
            cost_df=task_cost_data,
            title=title,
            group_des=group_by_description,
            color=color,
        )


def plotly_bar_cost(
    cost_df,
    title,
    group_des: Optional[bool] = False,
    color: Optional[str] = None,
) -> px.bar:
    """
    Plot the cost data
    :param cost_df: DataFrame with cost data
    :param title: Title for the plot
    :param group_des: Group the cost by description
    :param color: Color the bars by specified column
    :return: A plotly Figure object with the cost data plot.
    """

    fig = px.bar(
        cost_df,
        x="task_name",
        y="cost",
        color=color,
        title=title,
        hover_data="machine_spec",
    )

    fig.update_layout(
        xaxis_title="Task Name",
        yaxis_title="Cost",
        legend_title="Cost Description",
    )
    if group_des:
        fig.update_layout(
            barmode="group",
            bargap=0.15,  # gap between bars of adjacent location coordinates.
            bargroupgap=0.1,  # gap between bars of the same location coordinate.
        )

    return fig