from google.cloud.bigquery import SchemaField

RUNTIME_SCHEMA = [
    SchemaField(name="project_id", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="zone", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="instance_id", field_type="INTEGER", mode="REQUIRED"),
    SchemaField(name="instance_name", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="preemptible", field_type="BOOLEAN", mode="REQUIRED"),
    SchemaField(name="workflow_id", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="task_call_name", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="shard", field_type="INTEGER", mode="NULLABLE"),
    SchemaField(name="attempt", field_type="INTEGER", mode="NULLABLE"),
    SchemaField(name="cpu_count", field_type="INTEGER", mode="REQUIRED"),
    SchemaField(name="cpu_platform", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="mem_total_gb", field_type="FLOAT", mode="REQUIRED"),
    SchemaField(name="disk_mounts", field_type="STRING", mode="REPEATED"),
    SchemaField(name="disk_total_gb", field_type="FLOAT", mode="REPEATED"),
    SchemaField(name="start_time", field_type="TIMESTAMP", mode="REQUIRED"),
]

METRICS_SCHEMA = [
    SchemaField(name="timestamp", field_type="TIMESTAMP", mode="REQUIRED"),
    SchemaField(name="instance_id", field_type="INTEGER", mode="REQUIRED"),
    SchemaField(name="cpu_used_percent", field_type="FLOAT", mode="REPEATED"),
    SchemaField(name="mem_used_gb", field_type="FLOAT", mode="REQUIRED"),
    SchemaField(name="disk_used_gb", field_type="FLOAT", mode="REPEATED"),
    SchemaField(name="disk_read_iops", field_type="FLOAT", mode="REPEATED"),
    SchemaField(name="disk_write_iops", field_type="FLOAT", mode="REPEATED"),
]

TERRA_GCP_BILLING_SCHEMA = [
    SchemaField('billing_account_id', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('service', 'RECORD', 'NULLABLE', None, '', (
    SchemaField('id', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('description', 'STRING', 'NULLABLE', None, '', (), None)), None),
    SchemaField('sku', 'RECORD', 'NULLABLE', None, '', (
    SchemaField('id', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('description', 'STRING', 'NULLABLE', None, '', (), None)), None),
    SchemaField('usage_start_time', 'TIMESTAMP', 'NULLABLE', None, '', (), None),
    SchemaField('usage_end_time', 'TIMESTAMP', 'NULLABLE', None, '', (), None),
    SchemaField('project', 'RECORD', 'NULLABLE', None, '', (
    SchemaField('id', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('number', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('name', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('labels', 'RECORD', 'REPEATED', None, '', (
    SchemaField('key', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('value', 'STRING', 'NULLABLE', None, '', (), None)), None),
    SchemaField('ancestry_numbers', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('ancestors', 'RECORD', 'REPEATED', None, '', (
    SchemaField('resource_name', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('display_name', 'STRING', 'NULLABLE', None, '', (), None)), None)),
                None), SchemaField('labels', 'RECORD', 'REPEATED', None, '', (
    SchemaField('key', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('value', 'STRING', 'NULLABLE', None, '', (), None)), None),
    SchemaField('system_labels', 'RECORD', 'REPEATED', None, '', (
    SchemaField('key', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('value', 'STRING', 'NULLABLE', None, '', (), None)), None),
    SchemaField('location', 'RECORD', 'NULLABLE', None, '', (
    SchemaField('location', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('country', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('region', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('zone', 'STRING', 'NULLABLE', None, '', (), None)), None),
    SchemaField('export_time', 'TIMESTAMP', 'NULLABLE', None, '', (), None),
    SchemaField('cost', 'FLOAT', 'NULLABLE', None, '', (), None),
    SchemaField('currency', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('currency_conversion_rate', 'FLOAT', 'NULLABLE', None, '', (), None),
    SchemaField('usage', 'RECORD', 'NULLABLE', None, '', (
    SchemaField('amount', 'FLOAT', 'NULLABLE', None, '', (), None),
    SchemaField('unit', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('amount_in_pricing_units', 'FLOAT', 'NULLABLE', None, '', (), None),
    SchemaField('pricing_unit', 'STRING', 'NULLABLE', None, '', (), None)), None),
    SchemaField('credits', 'RECORD', 'REPEATED', None, '', (
    SchemaField('name', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('amount', 'FLOAT', 'NULLABLE', None, '', (), None),
    SchemaField('full_name', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('id', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('type', 'STRING', 'NULLABLE', None, '', (), None)), None),
    SchemaField('invoice', 'RECORD', 'NULLABLE', None, '',
                (SchemaField('month', 'STRING', 'NULLABLE', None, '', (), None),),
                None),
    SchemaField('cost_type', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('adjustment_info', 'RECORD', 'NULLABLE', None, '', (
    SchemaField('id', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('description', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('mode', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('type', 'STRING', 'NULLABLE', None, '', (), None)), None),
    SchemaField('tags', 'RECORD', 'REPEATED', None, '', (
    SchemaField('key', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('value', 'STRING', 'NULLABLE', None, '', (), None),
    SchemaField('inherited', 'BOOLEAN', 'NULLABLE', None, '', (), None),
    SchemaField('namespace', 'STRING', 'NULLABLE', None, '', (), None)), None),
    SchemaField('cost_at_list', 'FLOAT', 'NULLABLE', None, None, (), None),
    SchemaField('transaction_type', 'STRING', 'NULLABLE', None, None, (), None),
    SchemaField('seller_name', 'STRING', 'NULLABLE', None, None, (), None)]
