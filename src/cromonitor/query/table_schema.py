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
SchemaField(name="billing_account_id", field_type="STRING", mode="NULLABLE"),
SchemaField(name="service", field_type="RECORD", mode="NULLABLE", fields=[
    SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="description", field_type="STRING", mode="NULLABLE"),
]),
SchemaField(name="sku", field_type="RECORD", mode="NULLABLE", fields=[
    SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="description", field_type="STRING", mode="NULLABLE"),
]),
SchemaField(name="usage_start_time", field_type="TIMESTAMP", mode="NULLABLE"),
SchemaField(name="usage_end_time", field_type="TIMESTAMP", mode="NULLABLE"),
SchemaField(name="project", field_type="RECORD", mode="NULLABLE", fields=[
    SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="number", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="labels", field_type="RECORD", mode="REPEATED", fields=[
        SchemaField(name="key", field_type="STRING", mode="NULLABLE"),
        SchemaField(name="value", field_type="STRING", mode="NULLABLE"),
    ]),
    SchemaField(name="ancestry_numbers", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="ancestors", field_type="RECORD", mode="REPEATED", fields=[
        SchemaField(name="resource_name", field_type="STRING", mode="NULLABLE"),
        SchemaField(name="display_name", field_type="STRING", mode="NULLABLE"),
    ]),
]),
SchemaField(name="labels", field_type="RECORD", mode="REPEATED", fields=[
    SchemaField(name="key", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="value", field_type="STRING", mode="NULLABLE"),
]),
SchemaField(name="system_labels", field_type="RECORD", mode="REPEATED", fields=[
    SchemaField(name="key", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="value", field_type="STRING", mode="NULLABLE"),
]),
SchemaField(name="location", field_type="RECORD", mode="NULLABLE", fields=[
    SchemaField(name="location", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="country", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="region", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="zone", field_type="STRING", mode="NULLABLE"),
]),
SchemaField(name="export_time", field_type="TIMESTAMP", mode="NULLABLE"),
SchemaField(name="cost", field_type="FLOAT", mode="NULLABLE"),
SchemaField(name="currency", field_type="STRING", mode="NULLABLE"),
SchemaField(name="currency_conversion_rate", field_type="FLOAT", mode="NULLABLE"),
SchemaField(name="usage", field_type="RECORD", mode="NULLABLE", fields=[
    SchemaField(name="amount", field_type="FLOAT", mode="NULLABLE"),
    SchemaField(name="unit", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="amount_in_pricing_units", field_type="FLOAT", mode="NULLABLE"),
    SchemaField(name="pricing_unit", field_type="STRING", mode="NULLABLE"),
]),
SchemaField(name="credits", field_type="RECORD", mode="REPEATED", fields=[
    SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="amount", field_type="FLOAT", mode="NULLABLE"),
    SchemaField(name="full_name", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
]),
SchemaField(name="invoice", field_type="RECORD", mode="NULLABLE", fields=[
    SchemaField(name="month", field_type="STRING", mode="NULLABLE"),
]),
SchemaField(name="cost_type", field_type="STRING", mode="NULLABLE"),
SchemaField(name="adjustment_info", field_type="RECORD", mode="NULLABLE", fields=[
    SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="description", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="mode", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
]),
SchemaField(name="tags", field_type="RECORD", mode="REPEATED", fields=[
    SchemaField(name="key", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="value", field_type="STRING", mode="NULLABLE"),
    SchemaField(name="inherited", field_type="BOOLEAN", mode="NULLABLE"),
    SchemaField(name="namespace", field_type="STRING", mode="NULLABLE"),
]),
SchemaField(name="cost_at_list", field_type="FLOAT", mode="NULLABLE"),
SchemaField(name="transaction_type", field_type="STRING", mode="NULLABLE"),
SchemaField(name="seller_name", field_type="STRING", mode="NULLABLE"),
]
