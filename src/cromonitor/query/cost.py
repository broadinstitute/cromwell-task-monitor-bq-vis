# This module contains the class and functions to query the cost of a workflow
# from bigquery.
from datetime import datetime, timedelta, timezone
from typing import Union

from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator

from typing import List
from google.cloud.bigquery import ScalarQueryParameter
from ..logging import logging as log

from .table_schema import TERRA_GCP_BILLING_SCHEMA
from .utils import (
    check_bq_table_exists,
    check_bq_table_schema,
    check_cost_to_query_bq,
    check_workflow_id_exists_in_bq,
)


def check_minimum_time_passed_since_workflow_completion(
    end_time: datetime, min_hours: int = 24
):
    """
    Make sure 24 hours have passed between job finish time and executing this command
    (finished time minus current time).

    :param end_time: Workflow completion time obtained from cromwell job metadata
    :param min_hours: Minimum hours after workflow finished performing a query
    :return:
    """

    delta = datetime.now(timezone.utc) - end_time

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
    ):
        self.end_time: datetime = end_time
        self.start_time: datetime = start_time
        self.bq_cost_table: str = bq_cost_table
        self.project_id: str = bq_cost_table.split(".")[0]
        self.bq_client: bigquery.Client = bigquery.Client(project=self.project_id)
        self.workflow_id: str = workflow_id
        self.query_template: str = self._create_cost_query()
        self.query_config: bigquery.QueryJobConfig = self._create_bq_query_job_config()
        self.query_results: Union[RowIterator, None] or None = None
        self.formatted_query_results: list[dict] or None = None

    def query_cost(self) -> Union[RowIterator, None]:
        """
        Execute the cost query in bigquery
        :return:
        """
        self._checks_before_querying_bigquery()

        query_job = self.bq_client.query(
            self.query_template, job_config=self.query_config
        )

        self.query_results = query_job.result()
        self.formatted_query_results = self._format_bq_cost_query_results()
        return self.query_results

    def query_string(self) -> str:
        """
        Get the query string with all parameters replaced by their value.
        :return: Query string
        """

        query_parameters: List[ScalarQueryParameter] = self.query_config.query_parameters
        dry_run_string: str = self.query_template

        # Adding '@' to the parameter name to match bq param naming convention
        params_dict = {"@" + param.name: param.value for param in query_parameters}

        for param_name, param_value in params_dict.items():
            dry_run_string = dry_run_string.replace(param_name, param_value)

        return dry_run_string

    def results(self) -> list[dict]:
        """
        Get the formatted query results
        :return: list[dict]
        """
        if self.formatted_query_results is None:
            log.handle_user_error(
                err=None,
                message="Expecting list but results are None. Try running the query "
                        "first."
            )
        else:
            return self.formatted_query_results


    def _create_bq_query_job_config(self, date_padding: int = 1) -> bigquery.QueryJobConfig:
        """
        Create BQ Job config to be used while executing a query.
        :param date_padding: Number of days to subtract from start and end dates
        :return: bigquery.QueryJobConfig
        """

        formatted_start_date = (
                self.start_time - timedelta(days=date_padding)
        ).strftime("%Y-%m-%d")
        formatted_end_date = (
                self.end_time + timedelta(days=date_padding)
        ).strftime("%Y-%m-%d")

        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    name="workflow_id", type_="STRING", value=f"%{self.workflow_id}%"
                ),
                bigquery.ScalarQueryParameter(
                    name="start_date", type_="STRING", value=formatted_start_date
                ),
                bigquery.ScalarQueryParameter(
                    name="end_date", type_="STRING", value=formatted_end_date
                ),
            ]
        )

    def _checks_before_querying_bigquery(self):
        check_minimum_time_passed_since_workflow_completion(end_time=self.end_time)
        check_bq_table_schema(
            bq_client=self.bq_client,
            table_id=self.bq_cost_table, expected_schema=TERRA_GCP_BILLING_SCHEMA
        )
        check_bq_table_exists(bq_client=self.bq_client, table_id=self.bq_cost_table)
        check_workflow_id_exists_in_bq(
            bq_client=self.bq_client,
            table_id=self.bq_cost_table,
            workflow_id=self.workflow_id,
        )
        check_cost_to_query_bq(
            bq_client=self.bq_client,
            query=self.query_template,
            job_config=self.query_config,
        )

    def _format_bq_cost_query_results(
        self, task_header: str = "task_name", cost_header: str = "cost"
    ) -> list[dict]:
        """
        Turns bq query result object into list[dict], with each item being a dictionary
        representing tasks and their cost of a workflow.
        :param task_header: What to name new column to holding task names
        :param cost_header: What to name new column to holding task cost
        :return: list[dict]
        """
        query_rows: list = [dict(row) for row in self.query_results]
        formatted_query_rows = []
        for row in query_rows:
            formatted_query_rows.append(
                {task_header: row.get("task_name"), cost_header: row.get("cost")}
            )

        return formatted_query_rows

    def _create_cost_query(self) -> str:
        """
        Create an SQL query to be executed in BQ to retrieve workflow
        cost breakdown per workflow task.
        :return:
        """

        return f"""
            SELECT
             project.id AS google_project_id,
             (SELECT value FROM UNNEST(labels) AS l WHERE l.key = 'cromwell-workflow-id') AS cromwell_id,  
             (SELECT value FROM UNNEST(labels) AS l WHERE l.key = 'terra-submission-id') AS submission_id, 
             (SELECT value FROM UNNEST(labels) AS l WHERE l.key = 'wdl-task-name') AS task_name,
             (SELECT value FROM UNNEST(system_labels) AS l WHERE l.key = 'compute.googleapis.com/machine_spec') AS machine_spec,
             (SELECT value FROM UNNEST(system_labels) AS l WHERE l.key = 'compute.googleapis.com/cores') AS machine_cores,
             (SELECT value FROM UNNEST(system_labels) AS l WHERE l.key = 'compute.googleapis.com/memory') AS machine_memory,
             usage_start_time,
             service.description AS cost_description,
             cost
            FROM {self.bq_cost_table} AS billing,
             UNNEST(labels) AS label
            WHERE
             cost > 0
             AND usage_start_time BETWEEN TIMESTAMP(@start_date) AND TIMESTAMP(@end_date)
             AND label.key IN ('cromwell-workflow-id', 'terra-submission-id')
             AND label.value LIKE @workflow_id
    """

