"""
This module contains utility functions for creating widgets
"""

import ipywidgets as widgets
from IPython.display import display


def create_task_selector(options: list) -> widgets.SelectMultiple:
    """
    Create a task selector widget
    :param options:
    :return:
    """
    # Create the SelectMultiple widget
    task_selector = create_multi_dropdown_selector(
        options=options, option_description="Select a task"
    )

    # Display the widget and select the tasks
    display(task_selector)

    return task_selector


def create_submission_selector(options: list) -> widgets.Combobox:
    """
    Create a submission selector widget
    :param options:
    :return:
    """
    # Create the SelectMultiple widget
    submission_selector = create_box_selector(
        options=options, option_description="Submission ID"
    )

    # Display the widget and select the tasks
    display(submission_selector)

    return submission_selector


def create_workflow_selector(options: list) -> widgets.Combobox:
    """
    Create a workflow selector widget
    :param options:
    :return:
    """
    # Create the SelectMultiple widget
    workflow_selector = create_box_selector(
        options=options, option_description="Workflow ID"
    )

    # Display the widget and select the tasks
    display(workflow_selector)

    return workflow_selector


def create_box_selector(
    options: list, option_description: str, option_placeholder: str = None
):
    """
    Create a box selector widget
    :param options:
    :param option_description:
    :param option_placeholder:
    :return:
    """
    if len(options) == 1:
        option_placeholder = options[0]

    if option_placeholder is None:
        option_placeholder = "Select an option"

    # Create the SelectMultiple widget
    selected_submission_id = widgets.Combobox(
        placeholder=option_placeholder,
        options=options,
        description=option_description,
        ensure_option=True,
        disabled=False,
    )

    return selected_submission_id


def create_multi_dropdown_selector(
    options: list, option_description: str, option_placeholder: str = None
):
    """
    Create a multi dropdown selector widget
    :param options:
    :param option_description:
    :param option_placeholder:
    :return:
    """
    if len(options) == 1:
        option_placeholder = options[0]

    if option_placeholder is None:
        option_placeholder = "Select an option"

    # Create the SelectMultiple widget
    selected_submission_id = widgets.SelectMultiple(
        option_placeholder=option_placeholder,
        options=options,
        description=option_description,
        disabled=False,
        ensure_option=True,
    )

    return selected_submission_id
