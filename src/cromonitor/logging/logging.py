"""
This module contains functions to handle errors and warnings.
"""

import logging


def handle_value_error(err):
    """
    Handle a ValueError
    :param err:
    :return:
    """
    logging.error("A ValueError occurred: %s", err)


def handle_type_error(err):
    """
    Handle a TypeError
    """
    logging.error("A TypeError occurred: %s", err)


def handle_runtime_error(err):
    """
    Handle a RuntimeError
    :param err:
    :return:
    """
    logging.error("A RuntimeError occurred: %s", err)


def handle_firecloud_server_error(err, message):
    """
    Handle an error from the FireCloud server
    """
    logging.error("FISS ERROR: %s: %s", message, err)


def handle_bq_error(err, message):
    """
    Handle an error from BigQuery
    :param err:
    :param message:
    :return:
    """
    logging.error("BigQuery ERROR: %s: %s", message, err)


# Warnings


def handle_value_warning(err, message):
    """
    Handle a ValueError
    :param err:
    :param message:
    :return:
    """
    logging.warning("Value WARNING: %s: %s", message, err)


def handle_type_warning(err, message):
    """
    Handle a TypeError
    """
    logging.warning("Type WARNING: %s: %s", message, err)


def handle_bq_warning(err, message):
    """
    Handle a warning from BigQuery
    :param err:
    :param message:
    :return:
    """
    logging.warning("BigQuery WARNING: %s: %s", message, err)
