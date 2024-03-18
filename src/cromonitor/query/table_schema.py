from google.cloud.bigquery import SchemaField

RUNTIME_SCHEMA = [
    SchemaField("project_id", "STRING", "REQUIRED"),
    SchemaField("zone", "STRING", "REQUIRED"),
    SchemaField("instance_id", "INTEGER", "REQUIRED"),
    SchemaField("instance_name", "STRING", "REQUIRED"),
    SchemaField("preemptible", "BOOLEAN", "REQUIRED"),
    SchemaField("workflow_id", "STRING", "NULLABLE"),
    SchemaField("task_call_name", "STRING", "NULLABLE"),
    SchemaField("shard", "INTEGER", "NULLABLE"),
    SchemaField("attempt", "INTEGER", "NULLABLE"),
    SchemaField("cpu_count", "INTEGER", "REQUIRED"),
    SchemaField("cpu_platform", "STRING", "REQUIRED"),
    SchemaField("mem_total_gb", "FLOAT", "REQUIRED"),
    SchemaField("disk_mounts", "STRING", "REPEATED"),
    SchemaField("disk_total_gb", "FLOAT", "REPEATED"),
    SchemaField("start_time", "TIMESTAMP", "REQUIRED"),
]

METRICS_SCHEMA = [
    SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    SchemaField("instance_id", "INTEGER", "REQUIRED"),
    SchemaField("cpu_used_percent", "FLOAT", "REPEATED"),
    SchemaField("mem_used_gb", "FLOAT", "REQUIRED"),
    SchemaField("disk_used_gb", "FLOAT", "REPEATED"),
    SchemaField("disk_read_iops", "FLOAT", "REPEATED"),
    SchemaField("disk_write_iops", "FLOAT", "REPEATED"),
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
    SchemaField("usage_end_time", "TIMESTAMP", "NULLABLE",
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
            SchemaField("location", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("country", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("region", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("zone", "STRING", "NULLABLE", None, None, (), None),
        ),
    ),
    SchemaField(
        "resource",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            SchemaField("name", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("global_name", "STRING", "NULLABLE", None, None, (), None),
        ),
        None,
    ),
    SchemaField("export_time", "TIMESTAMP", "NULLABLE", None, None, (), None),
    SchemaField("cost", "FLOAT", "NULLABLE", None, None, (), None),
    SchemaField("currency", "STRING", "NULLABLE", None, None, (), None),
    SchemaField("currency_conversion_rate", "FLOAT", "NULLABLE", None, None, (), None),
    SchemaField(
        "usage",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            SchemaField("amount", "FLOAT", "NULLABLE", None, None, (), None),
            SchemaField("unit", "STRING", "NULLABLE", None, None, (), None),
            SchemaField(
                "amount_in_pricing_units", "FLOAT", "NULLABLE", None, None, (), None
            ),
            SchemaField("pricing_unit", "STRING", "NULLABLE", None, None, (), None),
        ),
        None,
    ),
    SchemaField(
        "credits",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            SchemaField("name", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("amount", "FLOAT", "NULLABLE", None, None, (), None),
            SchemaField("full_name", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("id", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("type", "STRING", "NULLABLE", None, None, (), None),
        ),
        None,
    ),
    SchemaField(
        "invoice",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (SchemaField("month", "STRING", "NULLABLE", None, None, (), None),),
        None,
    ),
    SchemaField("cost_type", "STRING", "NULLABLE", None, None, (), None),
    SchemaField(
        "adjustment_info",
        "RECORD",
        "NULLABLE",
        None,
        None,
        (
            SchemaField("id", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("description", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("mode", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("type", "STRING", "NULLABLE", None, None, (), None),
        ),
        None,
    ),
    SchemaField("partition_time", "TIMESTAMP", "NULLABLE", None, None, (), None),
    SchemaField("partition_date", "DATE", "NULLABLE", None, None, (), None),
    SchemaField(
        "tags",
        "RECORD",
        "REPEATED",
        None,
        None,
        (
            SchemaField("key", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("value", "STRING", "NULLABLE", None, None, (), None),
            SchemaField("inherited", "BOOLEAN", "NULLABLE", None, None, (), None),
        ),
        None,
    ),
]
