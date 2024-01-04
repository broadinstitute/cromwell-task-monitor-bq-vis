import logging
import sys
import datetime

import google.auth
from google.cloud import bigquery

import concurrent.futures

import pandas as pd


class QueryBQToMonitor:
    """
    The QueryBQToMonitor class contains the query scripts for the three different
    BQ tables (Metrics, Runtime, Cromwell Metadata). Its takes in a workflow ID and IDs
    of subworkflows (if any) and estimated dates when the job was submitted and
    successfully finished. It uses these parameters to query the
    BQ tables and produces a pandas datafram.
    """

    def __init__(self, workflow_ids, days_back_upper_bound, days_back_lower_bound,
                 bq_goolge_project):

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        h = logging.StreamHandler(sys.stderr)
        h.flush = sys.stderr.flush
        self.logger.addHandler(h)

        self.formated_workflow_ids = '\"' + '","'.join(
            workflow_ids) + '\"'  # format workflow ids for query

        self.days_back_upper_bound = days_back_upper_bound
        self.days_back_lower_bound = days_back_lower_bound

        self.bq_goolge_project = bq_goolge_project

        # Explicitly create a credentials object. This allows you to use the same
        # credentials for both the BigQuery and BigQuery Storage clients, avoiding
        # unnecessary API calls to fetch duplicate authentication tokens.
        credentials, project_id = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        # Make clients.
        self.bq_client = bigquery.Client(credentials=credentials, project=project_id)

    def query(self):
        self._get_runtime_and_metadata()
        self._get_metrics()

    def _get_runtime_and_metadata(self):

        self._fetch_runtime()
        self._fetch_metadata()

        runtime_nrow, runtime_ncol = self.runtime.shape
        meta_nrow, meta_ncol = self.metadata.shape

        # basic QC
        if (meta_nrow != runtime_nrow):
            self.logger.warning(
                'Metadata and runtime number of rows are different. You might want to check.')
        summary_msg = f"Nrows of runtime: {runtime_nrow}, Ncols of runtime: {runtime_ncol}, \nNrows of meta: {meta_nrow}, Ncols of meta: {meta_ncol}"
        self.logger.info(summary_msg)

        # merge the two
        self.metadata_runtime = pd.merge(self.metadata, self.runtime,
                                         left_on='meta_instance_name',
                                         right_on='runtime_instance_name', how='right')
        print()
        self.metadata_runtime.runtime_task_call_name.describe()

    def _fetch_runtime(self):
        # query runtime data
        runtime_sql = f"""

        SELECT

          runtime.attempt AS runtime_attempt,
          runtime.cpu_count AS runtime_cpu_count,
          runtime.cpu_platform AS runtime_cpu_platform,
          runtime.disk_mounts AS runtime_disk_mounts,
          runtime.disk_total_gb AS runtime_disk_total_gb,
          runtime.instance_id AS runtime_instance_id,
          runtime.instance_name AS runtime_instance_name,
          runtime.mem_total_gb AS runtime_mem_total_gb,
          runtime.preemptible AS runtime_preemptible,
          runtime.project_id AS runtime_project_id,
          runtime.shard AS runtime_shard,
          runtime.start_time AS runtime_start_time,
          runtime.task_call_name AS runtime_task_call_name,
          runtime.workflow_id AS runtime_workflow_id,
          runtime.zone AS runtime_zone

        FROM
          `{self.bq_goolge_project}.cromwell_monitoring.runtime`  runtime 

        WHERE
              DATE(runtime.start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {self.days_back_upper_bound} DAY)
          AND DATE(runtime.start_time) <= DATE_SUB(CURRENT_DATE(), INTERVAL {self.days_back_lower_bound} DAY)

          AND runtime.workflow_id IN ({self.formated_workflow_ids})    
        """
        self.runtime = self.bq_client.query(query=runtime_sql).to_dataframe()
        self.logger.info("Fetched runtime table.")

    def _fetch_metadata(self):
        # query metadata table
        metadata_sql = f"""

        SELECT
          metadata.attempt AS meta_attempt,
          metadata.cpu_count AS meta_cpu,
          metadata.disk_mounts AS meta_disk_mounts,
          metadata.disk_total_gb AS meta_disk_total_gb,
          metadata.disk_types AS meta_disk_types,
          metadata.docker_image AS meta_docker_image,
          metadata.end_time AS meta_end_time,
          metadata.execution_status AS meta_execution_status,
          metadata.inputs AS meta_inputs,
          metadata.instance_name AS meta_instance_name,
          metadata.mem_total_gb AS meta_mem_total_gb,
          metadata.preemptible AS meta_preemptible,
          metadata.project_id AS meta_project_id,
          metadata.shard AS meta_shard,
          metadata.start_time AS meta_start_time,
          metadata.task_call_name AS meta_task_call_name,
          metadata.workflow_id AS meta_workflow_id,
          metadata.workflow_name AS meta_workflow_name,
          metadata.zone AS meta_zone,
          TIMESTAMP_DIFF(metadata.end_time, metadata.start_time, SECOND) meta_duration_sec

        FROM
          `{self.bq_goolge_project}.cromwell_monitoring.metadata` metadata

        WHERE
              DATE(metadata.start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {self.days_back_upper_bound} DAY)
          AND DATE(metadata.start_time) <= DATE_SUB(CURRENT_DATE(), INTERVAL {self.days_back_lower_bound} DAY)

          AND metadata.workflow_id IN ({self.formated_workflow_ids})    
        """
        self.metadata = self.bq_client.query(query=metadata_sql).to_dataframe()
        self.logger.info("Fetched metadata table")

    def _get_metrics(self):

        # Log start time
        start_time = datetime.datetime.now().strftime("%H:%M:%S")
        self.logger.info(f"Started querying metrics on {start_time}.")

        # Retrieve instance IDs and set up parameters
        instance_ids = list(self.runtime.runtime_instance_id.unique())
        num_threads = 8  # Number of threads for concurrent processing
        batch_size = len(instance_ids) // num_threads

        # Split instance IDs into pools for concurrent processing
        ids_pool = {
            i: instance_ids[i * batch_size: (
                                                        i + 1) * batch_size] if i < num_threads else instance_ids[
                                                                                                     i * batch_size:]
            for i in range(num_threads + 1)
        }

        # Submit jobs to ThreadPoolExecutor for fetching metrics
        with concurrent.futures.ThreadPoolExecutor() as executor:
            jobs_pool = {
                i: executor.submit(self._fetch_metrics_on_vms_batch, ids_pool[i]) for i
                in range(num_threads + 1)}

        # Collect results from jobs
        results_pool = {i: jobs_pool[i].result() for i in range(num_threads + 1)}

        current_time = datetime.datetime.now()
        finish_time = current_time.strftime("%H:%M:%S")
        pf = current_time - datetime.datetime.strptime(start_time, "%H:%M:%S")
        s = pf.seconds
        hours, remainder = divmod(s, 3600)
        minutes, seconds = divmod(remainder, 60)
        elapse = '{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))
        self.logger.info(f"Finished on {finish_time}.")
        self.logger.info(f"Totalling {elapse}.")

        l = list(results_pool.values())
        self.metrics = pd.concat(l)

        # QC
        # Error if metrics is empty
        if (self.metrics.empty):
            self.logger.error("Error: Metrics table is empty. Please verify workflow "
                              "id or timeframe.")
            return
        # Warning if there are missing metrics
        retries = 0
        d = set(self.metadata_runtime.runtime_instance_id.unique()) - set(
            self.metrics.metrics_instance_id.unique())
        while ((not d) and 10 > retries):
            self.logger.info(f"Retrieving metrics info on leftovers: {d}")
            left_over = self._fetch_metrics_on_vms_batch(d)
            if (not left_over.empty):
                self.metrics = pd.concat([self.metrics, left_over], axis=0)
            d = set(self.metadata_runtime.runtime_instance_id.unique()) - set(
                self.metrics.metrics_instance_id.unique())
            retries += 1
        if (0 != d):
            self.logger.warning(
                f"Not all VMs provisioned have their metrics sent over ({d} didn't).")

    def _fetch_metrics_on_vms_batch(self, vm_instance_ids):
        """
        Fetches metrics on a batch of VMs.
        @param vm_instance_ids:
        @return:
        """
        # if vm_instance_ids is empty, return empty dataframe
        if (0 == len(vm_instance_ids)):
            return pd.DataFrame()

        ids_string = ', '.join(map(str, vm_instance_ids))
        metrics_sql = f"""

        SELECT

          metrics.cpu_used_percent AS metrics_cpu_used_percent,
          metrics.disk_read_iops AS metrics_disk_read_iops,
          metrics.disk_used_gb AS metrics_disk_used_gb,
          metrics.disk_write_iops AS metrics_disk_write_iops,
          metrics.instance_id AS metrics_instance_id,
          metrics.mem_used_gb AS metrics_mem_used_gb,
          metrics.timestamp AS metrics_timestamp

        FROM
          `{self.bq_goolge_project}.cromwell_monitoring.metrics`  metrics

        WHERE
          DATE(metrics.timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {self.days_back_upper_bound} DAY)
          AND DATE(metrics.timestamp) <= DATE_SUB(CURRENT_DATE(), INTERVAL {self.days_back_lower_bound} DAY)
          AND metrics.instance_id IN ({ids_string})

        """
        return self.bq_client.query(metrics_sql).to_dataframe()