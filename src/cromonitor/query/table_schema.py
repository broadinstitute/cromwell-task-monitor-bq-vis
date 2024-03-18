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
    SchemaField("billing_account_id", "STRING", "NULLABLE"),
    SchemaField(
        name="service",
        field_type="RECORD",
        mode="NULLABLE",
        fields=(
            SchemaField("id", "STRING", "NULLABLE"),
            SchemaField("description", "STRING", "NULLABLE"),
        ),
    ),
    SchemaField(
        name="sku",
        field_type="RECORD",
        mode="NULLABLE",
        fields=(
            SchemaField("id", "STRING", "NULLABLE"),
            SchemaField("description", "STRING", "NULLABLE"),
        ),

    ),
    SchemaField("usage_start_time", "TIMESTAMP", "NULLABLE"),
    SchemaField("usage_end_time", "TIMESTAMP", "NULLABLE"),
    SchemaField(
        name="project",
        field_type="RECORD",
        mode="NULLABLE",
        fields=(
            SchemaField("id", "STRING", "NULLABLE"),
            SchemaField("number", "STRING", "NULLABLE"),
            SchemaField("name", "STRING", "NULLABLE"),
            SchemaField(
                name="labels",
                field_type="RECORD",
                mode="REPEATED",
                fields=(
                    SchemaField("key", "STRING", "NULLABLE"),
                    SchemaField("value", "STRING", "NULLABLE"),
                ),
            ),
            SchemaField("ancestry_numbers", "STRING", "NULLABLE"),
            SchemaField(
                name="ancestors",
                field_type="RECORD",
                mode="REPEATED",
                fields=(
                    SchemaField(
                        "resource_name", "STRING", "NULLABLE"),
                    SchemaField(
                        "display_name", "STRING", "NULLABLE"),
                ),
            ),
        ),
    ),
    SchemaField(
        name="labels",
        field_type="RECORD",
        mode="REPEATED",
        fields=(
            SchemaField("key", "STRING", "NULLABLE"),
            SchemaField("value", "STRING", "NULLABLE"),
        ),
    ),
    SchemaField(
        name="system_labels",
        field_type="RECORD",
        mode="REPEATED",
        fields=(
            SchemaField("key", "STRING", "NULLABLE"),
            SchemaField("value", "STRING", "NULLABLE"),
        ),
    ),
    SchemaField(
        name="location",
        field_type="RECORD",
        mode="NULLABLE",
        fields=(
            SchemaField("location", "STRING", "NULLABLE"),
            SchemaField("country", "STRING", "NULLABLE"),
            SchemaField("region", "STRING", "NULLABLE"),
            SchemaField("zone", "STRING", "NULLABLE"),
        ),
    ),
    SchemaField(
        name="resource",
        field_type="RECORD",
        mode="NULLABLE",
        fields=(
            SchemaField("name", "STRING", "NULLABLE"),
            SchemaField("global_name", "STRING", "NULLABLE"),
        ),
    ),
    SchemaField("export_time", "TIMESTAMP", "NULLABLE"),
    SchemaField("cost", "FLOAT", "NULLABLE"),
    SchemaField("currency", "STRING", "NULLABLE"),
    SchemaField("currency_conversion_rate", "FLOAT", "NULLABLE"),
    SchemaField(
        name="usage",
        field_type="RECORD",
        mode="NULLABLE",
        fields=(
            SchemaField("amount", "FLOAT", "NULLABLE"),
            SchemaField("unit", "STRING", "NULLABLE"),
            SchemaField(
                "amount_in_pricing_units", "FLOAT", "NULLABLE"),
            SchemaField("pricing_unit", "STRING", "NULLABLE"),
        ),
    ),
    SchemaField(
        name="credits",
        field_type="RECORD",
        mode="REPEATED",
        fields=(
            SchemaField("name", "STRING", "NULLABLE"),
            SchemaField("amount", "FLOAT", "NULLABLE"),
            SchemaField("full_name", "STRING", "NULLABLE"),
            SchemaField("id", "STRING", "NULLABLE"),
            SchemaField("type", "STRING", "NULLABLE"),
        ),
    ),
    SchemaField(
        name="invoice",
        field_type="RECORD",
        mode="NULLABLE",
        fields=(
            SchemaField("month", "STRING", "NULLABLE"),
        ),
    ),
    SchemaField("cost_type", "STRING", "NULLABLE"),
    SchemaField(
        name="adjustment_info",
        field_type="RECORD",
        mode="NULLABLE",
        fields=(
            SchemaField("id", "STRING", "NULLABLE"),
            SchemaField("description", "STRING", "NULLABLE"),
            SchemaField("mode", "STRING", "NULLABLE"),
            SchemaField("type", "STRING", "NULLABLE"),
        ),
    ),
    SchemaField("partition_time", "TIMESTAMP", "NULLABLE"),
    SchemaField("partition_date", "DATE", "NULLABLE"),
    SchemaField(
        name="tags",
        field_type="RECORD",
        mode="REPEATED",
        fields=(
            SchemaField("key", "STRING", "NULLABLE"),
            SchemaField("value", "STRING", "NULLABLE"),
            SchemaField("inherited", "BOOLEAN", "NULLABLE"),
        ),
    ),
]
