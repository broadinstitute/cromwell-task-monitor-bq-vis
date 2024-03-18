from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from cromonitor.logging import logging as log


def check_bq_query_for_errors(query_job: bigquery.QueryJob) -> None:
    """
    Checks query response for errors
    :param query_job: Response from the BQ query execution
    :return:
    """
    if query_job.errors:
        log.handle_bq_error(
            err=query_job.errors, message="Something went wrong with query"
        )


def check_bq_query_results(query_job: bigquery.QueryJob) -> None:
    """
    Checks the contents of the query result
    :param query_job: Response from the BQ query execution
    :return:
    """
    query_results = query_job.result()
    if query_results.total_rows == 0:
        log.handle_bq_error(
            err=None,  # No error, but no results
            message="No results found for the workflow.",
        )


def check_cost_to_query_bq(
    bq_client: bigquery.Client,
    query: str,
    job_config: bigquery.QueryJobConfig,
    warning_cost: float = 5,
    error_cost: float = 100,
):
    """
    check the cost of running bq query (utility function)
    :return:
    """

    query_cost = bq_query_cost_calculation(
        query=query, bq_client=bq_client, job_config=job_config
    )

    if query_cost > warning_cost:
        log.handle_bq_warning(err=None, message=f"Cost will be over ${warning_cost}")

    if query_cost > error_cost:
        log.handle_bq_error(
            err=None,  # No error, but cost is high
            message=f"The cost of the query is over ${error_cost}!",
        )


def get_bytes_for_query_dry_run(
    query, bq_client: bigquery.Client, job_config: bigquery.QueryJobConfig
) -> int:
    """
    Dry run the query to check the cost
    :return:
    """

    # Set the job configuration dry run to True and use_query_cache to False
    job_config.dry_run = True
    job_config.use_query_cache = False

    if job_config.dry_run is False:
        log.handle_bq_error(
            err=None, message="Dry run is not set to True for checking cost."
        )

    # TODO: Remove this line
    # job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    # Start the query, passing in the extra configuration.
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.

    return int(query_job.total_bytes_processed)


def bq_query_cost_calculation(
    query: str,
    bq_client: bigquery.Client,
    job_config: bigquery.QueryJobConfig,
    bq_ondemand_cost: float = 6.25,
) -> float:
    """
    Calculate the cost of running the query
    :return:
    """
    # get the bytes processed from the dry run
    bytes_processed = get_bytes_for_query_dry_run(
        query=query, bq_client=bq_client, job_config=job_config
    )

    # On-demand pricing here: https://cloud.google.com/bigquery/pricing#on_demand_pricing
    # ~$6 per TB for on-demand pricing
    # get the cost of running the query
    cost = bytes_processed / (1024 * 1024 * 1024 * 1024) * bq_ondemand_cost

    return cost


# check table schema (utility function)
def check_bq_table_schema(bq_client: bigquery.Client, table_id: str, expected_schema: dict) -> None:
    """
    Check the schema of the table in bigquery
    :return:
    """

    table = bq_client.get_table(table_id)  # Make an API request.

    # check if the schema of the table matches the expected schema
    if table.schema != expected_schema:
        log.handle_bq_warning(
            err=None,  # No error, but the schema is different
            message="The schema of the table is different than expected. Please "
            "create an issue ticket so we can update the schema.",
        )


def construct_bq_table_id(project_id: str, dataset_name: str, table_name: str) -> str:
    """
    Create the table id for bigquery
    :param project_id:
    :param dataset_name:
    :param table_name:
    :return:
    """
    return project_id + "." + dataset_name + "." + table_name


# check if the table exists in bigquery (utility function)
def check_bq_table_exists(bq_client: bigquery.Client, table_id: str) -> None:
    """
    Check if the table exists in bigquery
    :param table_id: The table id in bigquery
    :return:
    """

    try:
        bq_client.get_table(table_id)  # Make an API request.
    except NotFound as e:
        log.handle_bq_error(err=e, message="Table is not found.")


def check_workflow_id_exists_in_bq(
        bq_client: bigquery.Client, table_id: str, workflow_id: str
) -> bool:
    """
    Check if the workflow id exists in bigquery table
    :param bq_client:
    :param table_id: The table id in bigquery
    :param workflow_id: The workflow id to check
    :return:
    """

    # Gives a boolean result True if the workflow_id exists in the table False otherwise
    query = f"""
        SELECT EXISTS(SELECT 1 FROM {table_id} WHERE workflow_id = '{workflow_id}')
        """

    try:
        query_job = bq_client.query(query)
        results = query_job.result()  # Wait for results

        # Access the Boolean result directly
        workflow_exists: bool = results[0][
            0
        ]  # Assuming a single-column, single-row result

        return workflow_exists

    except Exception as e:
        log.handle_bq_error(
            err=e, message="Error checking if workflow id exists in BQ."
        )


def get_project_id_from_table_id(table_id: str) -> str:
    """
    Get the project id from the table id
    :param table_id: The table id in bigquery
    :return:
    """
    return table_id.split(".")[0]
