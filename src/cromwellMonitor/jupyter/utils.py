import ipywidgets as widgets


def create_task_selector(options):
    # Create the SelectMultiple widget
    task_selector = widgets.SelectMultiple(
        options=options,
        description='Tasks',
        disabled=False
    )

    # Display the widget and select the tasks
    display(task_selector)

    return task_selector
