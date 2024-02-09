import firecloud.api as fapi
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime
from ..logging import logging as log

SUBMISSION_COLUMNS = [
    'submitter', 'status', 'submissionId', 'submissionDate', 'workflowStatuses'
]
WORKFLOW_COLUMNS = ['workflowId', 'status', 'statusLastChangedDate', 'messages']


def get_list_of_submissions(
        workspace_namespace: str,
        workspace_name: str,
        submission_columns: list = None
) -> pd.DataFrame:
    """
    Get a list of submissions for a workspace
    @param workspace_namespace: the namespace of the workspace
    @param workspace_name: the name of the workspace
    @param submission_columns: columns to include in the submission dataframe
    @return: a dataframe of submissions
    """
    if submission_columns is None:
        submission_columns = SUBMISSION_COLUMNS

    try:
        response_list_submissions = fapi.list_submissions(
            namespace=workspace_namespace, workspace=workspace_name
        )
        print("Status code:", response_list_submissions.status_code)
        response_dict = response_list_submissions.json()
        submissions_df = pd.DataFrame(response_dict)

        submissions_df_sorted = process_fapi_response_df(
            df=submissions_df,
            date_columns=['submissionDate'],
            columns=submission_columns,
            sort_by='submissionDate'

        )

        return submissions_df_sorted

    except Exception as e:
        log.handle_firecloud_server_error(
            err=e,
            message=f"Error getting list of submissions for workspace {workspace_name}"
        )


def get_submission_workflow_ids(
        workspace_namespace: str,
        workspace_name: str,
        submission_id: str,
        workflow_columns=None
) -> pd.DataFrame:
    """
    Get a list of workflow ids for a submission
    @param workspace_namespace: the namespace of the workspace
    @param workspace_name: the name of the workspace
    @param submission_id: the submission id
    @param workflow_columns: columns to include in the workflow dataframe
    @return: a dataframe of workflow ids
    """
    if workflow_columns is None:
        workflow_columns = WORKFLOW_COLUMNS

    try:

        response_get_submission = fapi.get_submission(
            workspace=workspace_name,
            namespace=workspace_namespace,
            submission_id=submission_id
        )

        print("Status code:", response_get_submission.status_code)
        response_get_submission_json = response_get_submission.json()
        workflow_id_df = pd.DataFrame(response_get_submission_json['workflows'])

        workflow_id_df_sorted = process_fapi_response_df(
            df=workflow_id_df,
            date_columns=['statusLastChangedDate'],
            columns=workflow_columns,
            sort_by='statusLastChangedDate'
        )

        return workflow_id_df_sorted

    except Exception as e:
        log.handle_firecloud_server_error(
            err=e,
            message=f"Error getting workflow ids for submissions {submission_id}"
        )


def process_fapi_response_df(
        df: pd.DataFrame, date_columns: list, columns: list, sort_by: str
) -> pd.DataFrame:
    """
    Process the content of a response to a FireCloud API request

    @param df: the dataframe to process
    @param date_columns: A list of column to convert to datetime
    @param columns: the columns to include in the dataframe,
        these should be in the response content json as keys
    @param sort_by: the column to sort by
    @return: a dataframe

    """
    # Convert date_column to datetime
    for date_column in date_columns:
        df[date_column] = pd.to_datetime(df[date_column])

    # Extract only useful info and sort by date
    df_sorted = df[columns].sort_values(by=sort_by, ascending=False)

    return df_sorted


def _convert_to_datetime(date_string):
    date_object = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%fZ')
    return date_object


def _days_from_today(date):
    today = datetime.now()
    difference = today - date
    return difference.days


class Workflow:
    """
    A class to represent a workflow
    """

    def __init__(
            self,
            workspace_namespace: str,
            workspace_name: str,
            submission_id: str,
            parent_workflow_id: str
    ):
        """
        Initialize the workflow
        :param workspace_namespace:
        :param workspace_name:
        :param submission_id:
        :param parent_workflow_id:
        """
        self.workspace_namespace = workspace_namespace
        self.workspace_name = workspace_name
        self.submission_id = submission_id
        self.parent_workflow_id = parent_workflow_id
        self.workflow_metadata: Dict[str, Any] = self._get_workflow_metadata()
        self.subworkflow_ids = self._extract_subworkflow_ids(self.workflow_metadata)
        self.workflow_start_time = _convert_to_datetime(
            self.workflow_metadata['start']
        )
        self.workflow_end_time = _convert_to_datetime(
            self.workflow_metadata['end']
        )
        self.workfow_start_from_today = _days_from_today(self.workflow_start_time)
        self.workfow_end_from_today = _days_from_today(self.workflow_end_time)

    def get_workflow_metadata(self):
        return self.workflow_metadata

    def get_subworkflow_ids(self):
        return self.subworkflow_ids

    def _get_workflow_metadata(self) -> dict:
        """
        Get the metadata for a workflow
        @param workspace_namespace: the namespace of the workspace
        @param workspace_name: the name of the workspace
        @param submission_id: the submission id
        @param workflow_id: the workflow id
        @return: a dictionary of workflow metadata
        """
        response_workflow_metadata = fapi.get_workflow_metadata(
            namespace=self.workspace_namespace,
            workspace=self.workspace_name,
            submission_id=self.submission_id,
            workflow_id=self.parent_workflow_id
        )
        print("Status code:", response_workflow_metadata.status_code)
        response_workflow_metadata_json = response_workflow_metadata.json()

        return response_workflow_metadata_json

    def _extract_subworkflow_ids(
            self, workflow_metadata_dict: Dict[str, Any]
    ) -> List[str]:
        """Get associated subworkflow ids from workflow metadata
        @param workflow_metadata_dict: the workflow metadata dictionary
        @return: a list of subworkflow ids
        """
        if not isinstance(workflow_metadata_dict, dict):
            return []

        ids = [
            sub_workflow_metadata['id']
            for call_name, shards in workflow_metadata_dict.get('calls', {}).items()
            for shard in shards
            if (sub_workflow_metadata := shard.get('subWorkflowMetadata', {})).get('id')
        ]

        ids.extend(
            id
            for call_name, shards in workflow_metadata_dict.get('calls', {}).items()
            for shard in shards
            for id in self._extract_subworkflow_ids(
                shard.get('subWorkflowMetadata', {})
            )
        )

        return ids
