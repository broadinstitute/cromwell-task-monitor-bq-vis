from google.cloud.bigquery import SchemaField

RUNTIME_SCHEMA = [
    SchemaField('project_id', 'STRING', 'REQUIRED', None, None, (), None),
    SchemaField('zone', 'STRING', 'REQUIRED', None, None, (), None),
    SchemaField('instance_id', 'INTEGER', 'REQUIRED', None, None, (), None),
    SchemaField('instance_name', 'STRING', 'REQUIRED', None, None, (), None),
    SchemaField('preemptible', 'BOOLEAN', 'REQUIRED', None, None, (), None),
    SchemaField('workflow_id', 'STRING', 'NULLABLE', None, None, (), None),
    SchemaField('task_call_name', 'STRING', 'NULLABLE', None, None, (), None),
    SchemaField('shard', 'INTEGER', 'NULLABLE', None, None, (), None),
    SchemaField('attempt', 'INTEGER', 'NULLABLE', None, None, (), None),
    SchemaField('cpu_count', 'INTEGER', 'REQUIRED', None, None, (), None),
    SchemaField('cpu_platform', 'STRING', 'REQUIRED', None, None, (), None),
    SchemaField('mem_total_gb', 'FLOAT', 'REQUIRED', None, None, (), None),
    SchemaField('disk_mounts', 'STRING', 'REPEATED', None, None, (), None),
    SchemaField('disk_total_gb', 'FLOAT', 'REPEATED', None, None, (), None),
    SchemaField('start_time', 'TIMESTAMP', 'REQUIRED', None, None, (), None)
]

METRICS_SCHEMA = [
    SchemaField('timestamp', 'TIMESTAMP', 'REQUIRED', None, None, (), None),
    SchemaField('instance_id', 'INTEGER', 'REQUIRED', None, None, (), None),
    SchemaField('cpu_used_percent', 'FLOAT', 'REPEATED', None, None, (), None),
    SchemaField('mem_used_gb', 'FLOAT', 'REQUIRED', None, None, (), None),
    SchemaField('disk_used_gb', 'FLOAT', 'REPEATED', None, None, (), None),
    SchemaField('disk_read_iops', 'FLOAT', 'REPEATED', None, None, (), None),
    SchemaField('disk_write_iops', 'FLOAT', 'REPEATED', None, None, (), None)
]

TERRA_GCP_BILLING_SCHEMA = [
    SchemaField('billing_account_id', 'STRING', 'NULLABLE', None, None, (), None),
    SchemaField('service', 'RECORD', 'NULLABLE', None, None, (
        SchemaField('id', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('description', 'STRING', 'NULLABLE', None, None, (), None)
    ), None),
    SchemaField('sku', 'RECORD', 'NULLABLE', None, None, (
        SchemaField('id', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('description', 'STRING', 'NULLABLE', None, None, (), None)), None),
    SchemaField('usage_start_time', 'TIMESTAMP', 'NULLABLE', None, None, (), None),
    SchemaField('usage_end_time', 'TIMESTAMP', 'NULLABLE', None, None, (), None),
    SchemaField('project', 'RECORD', 'NULLABLE', None, None, (
        SchemaField('id', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('number', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('name', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('labels', 'RECORD', 'REPEATED', None, None, (
            SchemaField('key', 'STRING', 'NULLABLE', None, None, (), None),
            SchemaField('value', 'STRING', 'NULLABLE', None, None, (), None)), None),
        SchemaField('ancestry_numbers', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('ancestors', 'RECORD', 'REPEATED', None, None, (
            SchemaField('resource_name', 'STRING', 'NULLABLE', None, None, (), None),
            SchemaField('display_name', 'STRING', 'NULLABLE', None, None, (), None)),
                    None)),
                None),
    SchemaField('labels', 'RECORD', 'REPEATED', None, None, (
        SchemaField('key', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('value', 'STRING', 'NULLABLE', None, None, (), None)), None),
    SchemaField('system_labels', 'RECORD', 'REPEATED', None, None, (
        SchemaField('key', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('value', 'STRING', 'NULLABLE', None, None, (), None)), None),
    SchemaField('location', 'RECORD', 'NULLABLE', None, None, (
        SchemaField('location', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('country', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('region', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('zone', 'STRING', 'NULLABLE', None, None, (), None)), None),
    SchemaField('resource', 'RECORD', 'NULLABLE', None, None, (
        SchemaField('name', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('global_name', 'STRING', 'NULLABLE', None, None, (), None)), None),
    SchemaField('export_time', 'TIMESTAMP', 'NULLABLE', None, None, (), None),
    SchemaField('cost', 'FLOAT', 'NULLABLE', None, None, (), None),
    SchemaField('currency', 'STRING', 'NULLABLE', None, None, (), None),
    SchemaField('currency_conversion_rate', 'FLOAT', 'NULLABLE', None, None, (), None),
    SchemaField('usage', 'RECORD', 'NULLABLE', None, None, (
        SchemaField('amount', 'FLOAT', 'NULLABLE', None, None, (), None),
        SchemaField('unit', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('amount_in_pricing_units', 'FLOAT', 'NULLABLE', None, None, (),
                    None),
        SchemaField('pricing_unit', 'STRING', 'NULLABLE', None, None, (), None)), None),
    SchemaField('credits', 'RECORD', 'REPEATED', None, None, (
        SchemaField('name', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('amount', 'FLOAT', 'NULLABLE', None, None, (), None),
        SchemaField('full_name', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('id', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('type', 'STRING', 'NULLABLE', None, None, (), None)), None),
    SchemaField('invoice', 'RECORD', 'NULLABLE', None, None,
                (SchemaField('month', 'STRING', 'NULLABLE', None, None, (), None),),
                None),
    SchemaField('cost_type', 'STRING', 'NULLABLE', None, None, (), None),
    SchemaField('adjustment_info', 'RECORD', 'NULLABLE', None, None, (
        SchemaField('id', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('description', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('mode', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('type', 'STRING', 'NULLABLE', None, None, (), None)), None),
    SchemaField('partition_time', 'TIMESTAMP', 'NULLABLE', None, None, (), None),
    SchemaField('partition_date', 'DATE', 'NULLABLE', None, None, (), None),
    SchemaField('tags', 'RECORD', 'REPEATED', None, None, (
        SchemaField('key', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('value', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('inherited', 'BOOLEAN', 'NULLABLE', None, None, (), None)), None)
]
