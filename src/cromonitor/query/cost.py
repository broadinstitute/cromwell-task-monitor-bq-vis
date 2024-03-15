# This module contains the class and functions to query the cost of a workflow
# from bigquery.
from datetime import datetime, timezone, timedelta
from typing import Union

from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator

from .utils import check_bq_table_schema, check_bq_table_exists, \
    check_workflow_id_exists_in_bq, check_cost_to_query_bq
from .table_schema import TERRA_GCP_BILLING_SCHEMA


def check_minimum_time_passed_since_workflow_completion(end_time: datetime,
                                                        min_hours: int = 24):
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


def create_cost_query():
    """
    Create an SQL query to be executed in BQ to retrieve workflow
    cost breakdown per workflow task.
    :return:
    """
    return f"""
                    SELECT wfid.value as cromwell_workflow_id, service.description, task.value as task_name, sum(cost) as cost
                    FROM @bq_cost_table as billing, UNNEST(labels) as wfid, UNNEST(labels) as task
                    WHERE cost > 0
                    AND task.key LIKE "wdl-task-name"
                    AND wfid.key LIKE "cromwell-workflow-id"
                    AND wfid.value like @workflow_id
                    AND partition_time BETWEEN @start_date AND @end_date
                    GROUP BY 1,2,3
                    ORDER BY 4 DESC
                    """


class CostQuery:
    """
    Class for querying and holding the query results on cost.
    """

    def __init__(self,
                 workflow_id: str,
                 bq_cost_table: str,
                 start_time: datetime,
                 end_time: datetime,
                 ):
        self.end_time: datetime = end_time
        self.start_time: datetime = start_time
        self.bq_cost_table: str = bq_cost_table
        self.workflow_id: str = workflow_id
        self.query_string: str = create_cost_query()
        self.query_config: bigquery.QueryJobConfig = self.create_bq_query_job_config()
        self.query_results: Union[RowIterator, None] or None = None
        self.formatted_query_results: list[dict] or None = None

    def query_cost(self) -> Union[RowIterator, None]:
        """
        Execute the cost query in bigquery
        :return:
        """
        self.checks_before_querying_bigquery()

        client = bigquery.Client()
        query_job = client.query(self.query_string, job_config=self.query_config)
        self.query_results = query_job.result()
        self.formatted_query_results = self.format_bq_cost_query_results()
        return self.query_results

    def create_bq_query_job_config(self):
        """
           Create BQ Job config to be used while executing a query.
           :param workflow_id:
           :param start_date:
           :param end_date:
           :return:
           """
        formatted_start_date = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        formatted_end_date = self.end_time.strftime("%Y-%m-%d %H:%M:%S")

        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(name="bq_cost_table", type_="STRING",
                                              value=self.bq_cost_table),
                bigquery.ScalarQueryParameter(name="workflow_id", type_="STRING",
                                              value="%" + self.workflow_id),
                bigquery.ScalarQueryParameter(name="start_date", type_="STRING",
                                              value=formatted_start_date),
                bigquery.ScalarQueryParameter(name="end_date", type_="STRING",
                                              value=formatted_end_date),
            ]
        )

    def checks_before_querying_bigquery(self):
        check_minimum_time_passed_since_workflow_completion(end_time=self.end_time)
        check_bq_table_schema(table_id=self.bq_cost_table,
                              expected_schema=TERRA_GCP_BILLING_SCHEMA)
        check_bq_table_exists(table_id=self.bq_cost_table)
        check_workflow_id_exists_in_bq(table_id=self.bq_cost_table,
                                       workflow_id=self.workflow_id)
        check_cost_to_query_bq(project_id=self.bq_cost_table.split('.')[0],
                               query=self.query_string, job_config=self.query_config
                               )

    def format_bq_cost_query_results(self,
                                     task_header: str = "task_name",
                                     cost_header: str = "cost") -> list[dict]:
        """
            Turns bq query result object into list[dict], with each item being a dictionary
            representing tasks and their cost of a workflow. Returns only task and cost columns
            :param query_results: Query result from BQ
            :param task_header: What to name new column to holding task names
            :param cost_header: What to name new column to holding task cost
            :return:
            """
        query_rows: list = [dict(row) for row in self.query_results]
        formatted_query_rows = []
        for row in query_rows:
            formatted_query_rows.append(
                {task_header: row.get("task_name"), cost_header: row.get("cost")}
            )

        return formatted_query_rows
