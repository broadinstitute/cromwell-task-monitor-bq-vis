import logging


def handle_value_error(err):
    logging.error(f"A ValueError occurred: {err}")


def handle_type_error(err):
    logging.error(f"A TypeError occurred: {err}")


def handle_runtime_error(err):
    logging.error(f"A RuntimeError occurred: {err}")


def handle_firecloud_server_error(err, message):
    logging.error(f"FISS ERROR: {message}: {err}")


def handle_bq_error(err, message):
    logging.error(f"BigQuery ERROR: {message}: {err}")


# Warnings


def handle_value_warning(err, message):
    logging.warning(f"Value WARNING: {message}: {err}")


def handle_bq_warning(err, message):
    logging.warning(f"BigQuery WARNING: {message}: {err}")
