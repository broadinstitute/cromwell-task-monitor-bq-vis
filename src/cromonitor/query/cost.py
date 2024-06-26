# This module contains the class and functions to query the cost of a workflow
# from bigquery.
import logging
from datetime import date, datetime, timedelta
from typing import List, Union

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter
from google.cloud.bigquery.table import RowIterator

from ..logging import logging as log
from .table_schema import TERRA_GCP_BILLING_SCHEMA
from .utils import (
    bq_query_cost_calculation,
    check_bq_table_exists,
    check_bq_table_schema,
    check_cost_to_query_bq,
)


def check_minimum_time_passed_since_workflow_completion(
    end_time: datetime, min_hours: int = 24
) -> tuple[bool, timedelta]:
    """
    Make sure 24 hours have passed between job finish time and executing this command
    (finished time minus current time).

    :param end_time: Workflow completion time obtained from cromwell job metadata
    :param min_hours: Minimum hours after workflow finished performing a query
    :return:
    """

    delta = datetime.now() - end_time.replace(tzinfo=None)

    min_delta = timedelta(hours=min_hours)

    return (True, delta) if delta > min_delta else (False, delta)


class CostQuery:
    """
    Class for querying and holding the query results on cost.
    """

    def __init__(
        self,
        workflow_id: str,
        bq_cost_table: str,
        start_time: datetime,
        end_time: datetime,
        debug: bool = False,
    ):

        if not workflow_id:
            log.handle_user_error(err=None, message="workflow_id cannot be empty")
        if not bq_cost_table:
            log.handle_user_error(err=None, message="bq_cost_table cannot be empty")
        if not start_time:
            log.handle_user_error(err=None, message="start_time cannot be empty")
        if not end_time:
            log.handle_user_error(err=None, message="end_time cannot be empty")

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        if debug:
            self.logger.setLevel(logging.DEBUG)

        self.end_time: datetime = end_time
        self.start_time: datetime = start_time
        self.bq_cost_table: str = bq_cost_table
        self.project_id: str = bq_cost_table.split(".")[0]
        self.bq_client: bigquery.Client = bigquery.Client(project=self.project_id)
        self.workflow_id: str = workflow_id
        self.query_template: str = self._create_cost_query()
        self.query_config: bigquery.QueryJobConfig = self._create_bq_query_job_config()
        self.query_job: Union[bigquery.QueryJob, None] = None

    def query_cost(self) -> Union[RowIterator, None]:
        """
        Execute the cost query in bigquery
        :return:
        """
        logging.debug("Running checks before querying BigQuery.")
        self._checks_before_querying_bigquery()

        query_job: Union[bigquery.QueryJob, None] = None

        try:
            logging.debug("Executing the query.")
            query_job = self.bq_client.query(
                self.query_template, job_config=self.query_config
            )
        except Exception as e:
            log.handle_bq_error(err=e, message="Error while querying BigQuery")

        self.query_job = query_job
        logging.info("Query executed successfully.")
        logging.info(f"bytes processed: {query_job.total_bytes_processed}")
        logging.info(f"cost to query: {self.get_cost_to_query()}")

        return self.query_job

    def get_query_string(self) -> str:
        """
        Get the query string with all parameters replaced by their value.
        :return: Query string
        """

        query_parameters: List[ScalarQueryParameter] = (
            self.query_config.query_parameters
        )
        dry_run_string: str = self.query_template

        # Adding '@' to the parameter name to match bq param naming convention
        params_dict = {"@" + param.name: param.value for param in query_parameters}

        for param_name, param_value in params_dict.items():
            if isinstance(param_value, date):
                dry_run_string = dry_run_string.replace(
                    param_name, param_value.strftime("%Y-%m-%d")
                )
            elif isinstance(param_value, str):
                dry_run_string = dry_run_string.replace(param_name, param_value)
            else:
                log.handle_value_error(
                    err=f"Unexpected parameter type: {type(param_value)}"
                )

        return dry_run_string

    def results(self, to_dataframe: bool = False) -> list[dict] or pd.DataFrame:
        """
        Get the formatted query results as a list of dictionaries or as pandas dataframe
        :param to_dataframe: If True, return the results as a pandas dataframe
        :return: list[dict]
        """

        if self.query_job is None:
            log.handle_user_error(
                err=None,
                message="Expecting query job but it is None. Try running the query "
                "first.",
            )
        else:
            if to_dataframe:
                return self.query_job.to_dataframe()
            else:
                return self._format_bq_cost_query_results()

    def get_cost_to_query(self) -> float:
        """
        Get the cost of the query that was executed.
        The cost is calculated based on the amount of data processed by the query.
        The cost is calculated as follows:
        cost = bytes_processed / (1024 * 1024 * 1024 * 1024) * bq_ondemand_cost

        Where bq_ondemand_cost is the cost of running the query per TB, ~6.25.

        :return: Float
        """
        return bq_query_cost_calculation(
            query=self.query_template,
            bq_client=self.bq_client,
            job_config=self.query_config,
        )

    def _create_bq_query_job_config(
        self, date_padding: int = 2
    ) -> bigquery.QueryJobConfig:
        """
        Create BQ Job config to be used while executing a query.
        :param date_padding: Number of days to subtract from start and end dates
        :return: bigquery.QueryJobConfig
        """

        formatted_start_date = (
            self.start_time - timedelta(days=date_padding)
        ).strftime("%Y-%m-%d")
        formatted_end_date = (self.end_time + timedelta(days=date_padding)).strftime(
            "%Y-%m-%d"
        )

        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    name="workflow_id", type_="STRING", value=f"%{self.workflow_id}%"
                ),
                bigquery.ScalarQueryParameter(
                    name="start_date", type_="DATE", value=formatted_start_date
                ),
                bigquery.ScalarQueryParameter(
                    name="end_date", type_="DATE", value=formatted_end_date
                ),
            ]
        )

    def _checks_before_querying_bigquery(self):
        check_minimum_time_passed_since_workflow_completion(end_time=self.end_time)
        check_bq_table_schema(
            bq_client=self.bq_client,
            table_id=self.bq_cost_table,
            expected_schema=TERRA_GCP_BILLING_SCHEMA,
        )
        check_bq_table_exists(bq_client=self.bq_client, table_id=self.bq_cost_table)
        check_cost_to_query_bq(
            bq_client=self.bq_client,
            query=self.query_template,
            job_config=self.query_config,
        )

    def _format_bq_cost_query_results(self) -> list[dict]:
        """
        Turns bq query result object into list[dict], with each item being a dictionary
        representing tasks and their cost of a workflow.
        :return: List[dict]
        """

        return [dict(row) for row in self.query_job.result()]

    def _create_cost_query(self) -> str:
        """
        Create an SQL query to be executed in BQ to retrieve workflow
        cost breakdown per workflow task.
        :return:
        """

        return f"""
            SELECT
              -- Workflow details
              project.id AS google_project_id,
              (SELECT REGEXP_REPLACE(value, r'^terra-', '') FROM UNNEST(labels) AS l WHERE l.key = 'terra-submission-id') AS submission_id,
              (SELECT REGEXP_REPLACE(value, r'^cromwell-', '') FROM UNNEST(labels) AS l WHERE l.key = 'cromwell-workflow-id') AS workflow_id,
              (SELECT value FROM UNNEST(labels) AS l WHERE l.key = 'wdl-task-name') AS task_name,
              (SELECT value FROM UNNEST(labels) AS l WHERE l.key = 'cromwell-sub-workflow-name') AS subworkflow_name,
              (SELECT value FROM UNNEST(labels) AS l WHERE l.key = 'wdl-call-alias') AS task_alias,
              -- Cost breakdown
              service.description AS cost_service,
              sku.description AS cost_description,
              cost,
              -- Machine specs
              (SELECT value FROM UNNEST(system_labels) AS l WHERE l.key = 'compute.googleapis.com/machine_spec') AS machine_spec,
              (SELECT value FROM UNNEST(system_labels) AS l WHERE l.key = 'compute.googleapis.com/cores') AS machine_cores,
              (SELECT value FROM UNNEST(system_labels) AS l WHERE l.key = 'compute.googleapis.com/memory') AS machine_memory,
              usage_start_time,
              usage_end_time
            FROM {self.bq_cost_table} AS billing,
             UNNEST(labels) AS label
            WHERE
             cost > 0
             AND TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP(@start_date) AND TIMESTAMP(@end_date)
             AND label.key IN ('cromwell-workflow-id', 'terra-submission-id')
             AND label.value LIKE @workflow_id
    """
