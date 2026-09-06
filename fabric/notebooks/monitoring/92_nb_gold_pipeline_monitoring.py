# COMMAND ----------

# Cell 1
from pyspark.sql import functions as F
from delta.tables import DeltaTable

control = (
    "`BritMart Retail Data Platform - Dev`."
    "`wh_britmart_control`."
    "`ctl`"
)

pipeline_run = spark.table(
    f"{control}.`pipeline_run`"
)

entity_run = spark.table(
    f"{control}.`entity_run`"
)

print("=== CONTROL SOURCE COUNTS ===")
print("pipeline_run rows:", pipeline_run.count())
print("entity_run rows:", entity_run.count()) 

# COMMAND ----------

# Cell 2
from pyspark.sql import functions as F

# =====================================================
# GOLD PIPELINE RUN MONITORING
# Grain: one row per pipeline execution
# Adds monitoring_status so stale RUNNING records
# are classified correctly for reporting.
# =====================================================

gold_pipeline_run = (
    pipeline_run
    .select(
        F.hex("pipeline_run_id").alias("pipeline_run_id"),

        F.col("fabric_pipeline_run_id"),
        F.col("pipeline_name"),
        F.col("trigger_type"),
        F.col("trigger_name"),
        F.col("requested_load_type"),

        F.date_format(
            F.to_date("started_at_utc"),
            "yyyyMMdd"
        ).cast("int").alias("run_date_key"),

        F.col("window_start_utc"),
        F.col("window_end_utc"),

        # Preserve original audit status
        F.col("run_status"),

        # Derived monitoring status
        F.when(
            (F.col("run_status") == "RUNNING")
            & F.col("completed_at_utc").isNull()
            & (
                F.unix_timestamp(F.current_timestamp())
                - F.unix_timestamp(F.col("started_at_utc"))
                > 7200
            ),
            F.lit("STALE")
        )
        .otherwise(F.col("run_status"))
        .alias("monitoring_status"),

        F.col("started_at_utc"),
        F.col("completed_at_utc"),
        F.col("duration_seconds"),

        F.col("entities_requested"),
        F.col("entities_succeeded"),
        F.col("entities_failed"),
        F.col("entities_skipped"),

        F.col("error_code"),
        F.col("error_message"),
        F.col("initiated_by")
    )
)

# =====================================================
# VALIDATION
# =====================================================

source_count = pipeline_run.count()
gold_count = gold_pipeline_run.count()

duplicate_ids = (
    gold_pipeline_run
    .groupBy("pipeline_run_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_ids = (
    gold_pipeline_run
    .filter(F.col("pipeline_run_id").isNull())
    .count()
)

negative_duration = (
    gold_pipeline_run
    .filter(F.col("duration_seconds") < 0)
    .count()
)

bad_timestamp_order = (
    gold_pipeline_run
    .filter(
        F.col("completed_at_utc").isNotNull()
        & (
            F.col("completed_at_utc")
            < F.col("started_at_utc")
        )
    )
    .count()
)

invalid_entity_totals = (
    gold_pipeline_run
    .filter(
        F.col("entities_requested").isNotNull()
        &
        (
            F.coalesce(
                F.col("entities_succeeded"),
                F.lit(0)
            )
            +
            F.coalesce(
                F.col("entities_failed"),
                F.lit(0)
            )
            +
            F.coalesce(
                F.col("entities_skipped"),
                F.lit(0)
            )
            >
            F.col("entities_requested")
        )
    )
    .count()
)

# =====================================================
# OUTPUT
# =====================================================

print("=== GOLD PIPELINE RUN VALIDATION ===")
print("Source rows:", source_count)
print("Gold rows:", gold_count)
print("Row-count difference:", gold_count - source_count)
print("Duplicate pipeline run IDs:", duplicate_ids)
print("Null pipeline run IDs:", null_ids)
print("Negative durations:", negative_duration)
print("Completed before started:", bad_timestamp_order)
print("Invalid entity totals:", invalid_entity_totals)

print("\n=== ORIGINAL PIPELINE STATUS DISTRIBUTION ===")
gold_pipeline_run.groupBy("run_status").count().show()

print("\n=== MONITORING STATUS DISTRIBUTION ===")
gold_pipeline_run.groupBy("monitoring_status").count().show()

# COMMAND ----------

# Cell 3
from pyspark.sql import functions as F

# =====================================================
# GOLD ENTITY RUN MONITORING
# Grain: one row per entity execution
# Adds monitoring_status for operational reporting
# =====================================================

gold_entity_run = (
    entity_run
    .select(
        F.hex("entity_run_id").alias("entity_run_id"),
        F.hex("pipeline_run_id").alias("pipeline_run_id"),
        F.hex("ingestion_config_id").alias("ingestion_config_id"),

        F.col("activity_run_id"),
        F.col("entity_code"),
        F.col("load_type"),

        F.col("extraction_start_value"),
        F.col("extraction_end_value"),
        F.col("extraction_tie_breaker"),

        F.col("source_object_count"),
        F.col("source_row_count"),
        F.col("bronze_row_count"),
        F.col("rejected_row_count"),

        F.col("bytes_read"),
        F.col("bytes_written"),
        F.col("destination_path"),

        # Preserve original source audit status
        F.col("run_status"),

        # Derived operational monitoring status
        F.when(
            (F.col("run_status") == "RUNNING")
            & F.col("completed_at_utc").isNull()
            & (
                F.unix_timestamp(F.current_timestamp())
                - F.unix_timestamp(F.col("started_at_utc"))
                > 7200
            ),
            F.lit("STALE")
        )
        .otherwise(F.col("run_status"))
        .alias("monitoring_status"),

        F.col("attempt_number"),

        F.date_format(
            F.to_date("started_at_utc"),
            "yyyyMMdd"
        ).cast("int").alias("run_date_key"),

        F.col("started_at_utc"),
        F.col("completed_at_utc"),
        F.col("duration_seconds"),

        F.col("error_code"),
        F.col("error_category"),
        F.col("error_message")
    )
)

# =====================================================
# STRUCTURAL VALIDATION
# =====================================================

source_count = entity_run.count()
gold_count = gold_entity_run.count()

duplicate_ids = (
    gold_entity_run
    .groupBy("entity_run_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_ids = (
    gold_entity_run
    .filter(F.col("entity_run_id").isNull())
    .count()
)

null_parent_ids = (
    gold_entity_run
    .filter(F.col("pipeline_run_id").isNull())
    .count()
)

negative_duration = (
    gold_entity_run
    .filter(
        F.col("duration_seconds").isNotNull()
        & (F.col("duration_seconds") < 0)
    )
    .count()
)

bad_timestamp_order = (
    gold_entity_run
    .filter(
        F.col("completed_at_utc").isNotNull()
        & F.col("started_at_utc").isNotNull()
        & (
            F.col("completed_at_utc")
            < F.col("started_at_utc")
        )
    )
    .count()
)

negative_row_counts = (
    gold_entity_run
    .filter(
        (
            F.col("source_row_count").isNotNull()
            & (F.col("source_row_count") < 0)
        )
        |
        (
            F.col("bronze_row_count").isNotNull()
            & (F.col("bronze_row_count") < 0)
        )
        |
        (
            F.col("rejected_row_count").isNotNull()
            & (F.col("rejected_row_count") < 0)
        )
    )
    .count()
)

negative_bytes = (
    gold_entity_run
    .filter(
        (
            F.col("bytes_read").isNotNull()
            & (F.col("bytes_read") < 0)
        )
        |
        (
            F.col("bytes_written").isNotNull()
            & (F.col("bytes_written") < 0)
        )
    )
    .count()
)

invalid_attempt_numbers = (
    gold_entity_run
    .filter(
        F.col("attempt_number").isNotNull()
        & (F.col("attempt_number") < 1)
    )
    .count()
)

print("=== GOLD ENTITY RUN VALIDATION ===")
print("Source rows:", source_count)
print("Gold rows:", gold_count)
print("Row-count difference:", gold_count - source_count)
print("Duplicate entity run IDs:", duplicate_ids)
print("Null entity run IDs:", null_ids)
print("Null pipeline run IDs:", null_parent_ids)
print("Negative durations:", negative_duration)
print("Completed before started:", bad_timestamp_order)
print("Negative row counts:", negative_row_counts)
print("Negative byte counts:", negative_bytes)
print("Invalid attempt numbers:", invalid_attempt_numbers)

print("\n=== ORIGINAL ENTITY STATUS DISTRIBUTION ===")
gold_entity_run.groupBy("run_status").count().orderBy("run_status").show()

print("\n=== ENTITY MONITORING STATUS DISTRIBUTION ===")
gold_entity_run.groupBy("monitoring_status").count().orderBy("monitoring_status").show()

# COMMAND ----------

# Cell 4
from pyspark.sql import functions as F

# =====================================================
# GOLD MONITORING - RELATIONSHIP & BUSINESS DQ
# =====================================================

# -----------------------------------------------------
# 1. VALID PIPELINE IDS
# -----------------------------------------------------

valid_pipeline_ids = (
    gold_pipeline_run
    .select("pipeline_run_id")
    .filter(F.col("pipeline_run_id").isNotNull())
    .distinct()
)


# -----------------------------------------------------
# 2. VALID ENTITY RUNS
# -----------------------------------------------------

gold_entity_run_valid = (
    gold_entity_run.alias("e")
    .join(
        valid_pipeline_ids.alias("p"),
        "pipeline_run_id",
        "inner"
    )
)


# -----------------------------------------------------
# 3. ORPHAN ENTITY RUNS / QUARANTINE
# -----------------------------------------------------

gold_entity_run_rejected = (
    gold_entity_run.alias("e")
    .join(
        valid_pipeline_ids.alias("p"),
        "pipeline_run_id",
        "left_anti"
    )
    .withColumn(
        "dq_rejection_reason",
        F.lit("PARENT_PIPELINE_RUN_NOT_FOUND")
    )
    .withColumn(
        "dq_rejected_at_utc",
        F.current_timestamp()
    )
)


# -----------------------------------------------------
# 4. RELATIONSHIP COUNTS
# -----------------------------------------------------

source_entity_count = gold_entity_run.count()
valid_entity_count = gold_entity_run_valid.count()
rejected_entity_count = gold_entity_run_rejected.count()

reconciliation_difference = (
    source_entity_count
    - valid_entity_count
    - rejected_entity_count
)

print("=== GOLD ENTITY RELATIONSHIP CLASSIFICATION ===")
print("Source entity rows:", source_entity_count)
print("Valid entity rows:", valid_entity_count)
print("Rejected/orphan entity rows:", rejected_entity_count)
print("Reconciliation difference:", reconciliation_difference)


# -----------------------------------------------------
# 5. SUCCESSFUL ZERO / NULL BRONZE ROWS
# Informational only.
# Legitimate incremental loads can process zero rows.
# -----------------------------------------------------

successful_zero_bronze = (
    gold_entity_run_valid
    .filter(
        (F.upper(F.col("run_status")) == "SUCCESS")
        &
        (
            F.coalesce(
                F.col("bronze_row_count"),
                F.lit(0)
            ) == 0
        )
    )
    .count()
)


# -----------------------------------------------------
# 6. FAILED RUNS WITHOUT ERROR DETAILS
# -----------------------------------------------------

failed_without_error = (
    gold_entity_run_valid
    .filter(
        (F.upper(F.col("run_status")) == "FAILED")
        &
        F.col("error_code").isNull()
        &
        F.col("error_category").isNull()
        &
        F.col("error_message").isNull()
    )
    .count()
)


# -----------------------------------------------------
# 7. INVALID REJECTION COUNTS
# -----------------------------------------------------

invalid_rejection_counts = (
    gold_entity_run_valid
    .filter(
        F.col("source_row_count").isNotNull()
        &
        F.col("rejected_row_count").isNotNull()
        &
        (
            F.col("rejected_row_count")
            > F.col("source_row_count")
        )
    )
    .count()
)


# -----------------------------------------------------
# 8. COMPLETED STATUS WITHOUT COMPLETION TIMESTAMP
# -----------------------------------------------------

completed_without_timestamp = (
    gold_entity_run_valid
    .filter(
        F.upper(F.col("run_status")).isin(
            "SUCCESS",
            "FAILED",
            "SKIPPED"
        )
        &
        F.col("completed_at_utc").isNull()
    )
    .count()
)


# -----------------------------------------------------
# 9. RUNNING WITH COMPLETION TIMESTAMP
# -----------------------------------------------------

running_with_completion_timestamp = (
    gold_entity_run_valid
    .filter(
        (F.upper(F.col("run_status")) == "RUNNING")
        &
        F.col("completed_at_utc").isNotNull()
    )
    .count()
)


# -----------------------------------------------------
# 10. STALE MONITORING RECORD CHECK
# -----------------------------------------------------
# STALE should represent a source RUNNING record
# that has no completion timestamp.

invalid_stale_records = (
    gold_entity_run_valid
    .filter(
        (F.upper(F.col("monitoring_status")) == "STALE")
        &
        (
            (F.upper(F.col("run_status")) != "RUNNING")
            |
            F.col("completed_at_utc").isNotNull()
        )
    )
    .count()
)


# -----------------------------------------------------
# 11. OUTPUT
# -----------------------------------------------------

print("\n=== GOLD MONITORING BUSINESS CHECKS ===")

print(
    "Successful valid entity runs with zero/null Bronze rows:",
    successful_zero_bronze
)

print(
    "Failed runs without error details:",
    failed_without_error
)

print(
    "Rejected rows greater than source rows:",
    invalid_rejection_counts
)

print(
    "Completed runs without completion timestamp:",
    completed_without_timestamp
)

print(
    "RUNNING runs with completion timestamp:",
    running_with_completion_timestamp
)

print(
    "Invalid STALE monitoring records:",
    invalid_stale_records
)


# -----------------------------------------------------
# 12. VALID ENTITY MONITORING STATUS DISTRIBUTION
# -----------------------------------------------------

print("\n=== VALID ENTITY MONITORING STATUS DISTRIBUTION ===")

(
    gold_entity_run_valid
    .groupBy("monitoring_status")
    .count()
    .orderBy("monitoring_status")
    .show(truncate=False)
)


# -----------------------------------------------------
# 13. HARD DQ RESULT FOR CURATED GOLD
# -----------------------------------------------------

hard_failure_count = (
    failed_without_error
    + invalid_rejection_counts
    + completed_without_timestamp
    + running_with_completion_timestamp
    + invalid_stale_records
    + abs(reconciliation_difference)
)

print("\n=== CURATED GOLD MONITORING DQ RESULT ===")
print("Hard failure count:", hard_failure_count)

if hard_failure_count == 0:
    print(
        "PASS: Valid Gold monitoring records passed "
        "relationship and business DQ checks."
    )
else:
    print(
        "FAIL: Curated Gold monitoring records require investigation."
    )


# -----------------------------------------------------
# 14. SHOW QUARANTINED RECORDS
# -----------------------------------------------------

print("\n=== QUARANTINED MONITORING RECORDS ===")

(
    gold_entity_run_rejected
    .select(
        "pipeline_run_id",
        "entity_run_id",
        "entity_code",
        "load_type",
        "run_status",
        "monitoring_status",
        "started_at_utc",
        "completed_at_utc",
        "dq_rejection_reason",
        "dq_rejected_at_utc"
    )
    .orderBy("started_at_utc")
    .show(100, truncate=False)
)

# COMMAND ----------

# Cell 5
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# =====================================================
# GOLD MONITORING - FINAL PERSISTENCE
# =====================================================


# =====================================================
# 1. UPSERT GOLD_PIPELINE_RUN
# =====================================================

pipeline_target = "gold_pipeline_run"

if spark.catalog.tableExists(pipeline_target):

    pipeline_delta = DeltaTable.forName(
        spark,
        pipeline_target
    )

    (
        pipeline_delta.alias("t")
        .merge(
            gold_pipeline_run.alias("s"),
            "t.pipeline_run_id = s.pipeline_run_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("gold_pipeline_run MERGE completed.")

else:

    (
        gold_pipeline_run
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(pipeline_target)
    )

    print("gold_pipeline_run created.")


# =====================================================
# 2. UPSERT VALID GOLD_ENTITY_RUN
# =====================================================

entity_target = "gold_entity_run"

if spark.catalog.tableExists(entity_target):

    entity_delta = DeltaTable.forName(
        spark,
        entity_target
    )

    (
        entity_delta.alias("t")
        .merge(
            gold_entity_run_valid.alias("s"),
            "t.entity_run_id = s.entity_run_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("gold_entity_run MERGE completed.")

else:

    (
        gold_entity_run_valid
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(entity_target)
    )

    print("gold_entity_run created.")


# =====================================================
# 3. UPSERT QUARANTINED ENTITY RUNS
# =====================================================

rejected_target = "gold_entity_run_rejected"

if spark.catalog.tableExists(rejected_target):

    rejected_delta = DeltaTable.forName(
        spark,
        rejected_target
    )

    (
        rejected_delta.alias("t")
        .merge(
            gold_entity_run_rejected.alias("s"),
            "t.entity_run_id = s.entity_run_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("gold_entity_run_rejected MERGE completed.")

else:

    (
        gold_entity_run_rejected
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(rejected_target)
    )

    print("gold_entity_run_rejected created.")


# =====================================================
# 4. RELOAD PERSISTED TABLES
# =====================================================

pipeline_persisted = spark.table(
    pipeline_target
)

entity_persisted = spark.table(
    entity_target
)

rejected_persisted = spark.table(
    rejected_target
)


# =====================================================
# 5. PRIMARY KEY CHECKS
# =====================================================

pipeline_duplicate_ids = (
    pipeline_persisted
    .groupBy("pipeline_run_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

entity_duplicate_ids = (
    entity_persisted
    .groupBy("entity_run_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

rejected_duplicate_ids = (
    rejected_persisted
    .groupBy("entity_run_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

pipeline_null_ids = (
    pipeline_persisted
    .filter(F.col("pipeline_run_id").isNull())
    .count()
)

entity_null_ids = (
    entity_persisted
    .filter(F.col("entity_run_id").isNull())
    .count()
)

rejected_null_ids = (
    rejected_persisted
    .filter(F.col("entity_run_id").isNull())
    .count()
)


# =====================================================
# 6. VALID ENTITY -> PIPELINE RELATIONSHIP
# =====================================================

entity_parent_orphans = (
    entity_persisted
    .filter(F.col("pipeline_run_id").isNotNull())
    .select("pipeline_run_id")
    .distinct()
    .join(
        pipeline_persisted
        .select("pipeline_run_id")
        .distinct(),
        "pipeline_run_id",
        "left_anti"
    )
    .count()
)


# =====================================================
# 7. VALID / REJECTED OVERLAP
# =====================================================

valid_rejected_overlap = (
    entity_persisted
    .select("entity_run_id")
    .distinct()
    .join(
        rejected_persisted
        .select("entity_run_id")
        .distinct(),
        "entity_run_id",
        "inner"
    )
    .count()
)


# =====================================================
# 8. CURRENT SOURCE CLASSIFICATION
# =====================================================

source_entity_rows = gold_entity_run.count()
current_valid_rows = gold_entity_run_valid.count()
current_rejected_rows = gold_entity_run_rejected.count()

classification_difference = (
    source_entity_rows
    - current_valid_rows
    - current_rejected_rows
)


# =====================================================
# 9. FINAL OUTPUT
# =====================================================

print("\n=== GOLD MONITORING FINAL PERSISTENCE VALIDATION ===")

print(
    "Persisted pipeline rows:",
    pipeline_persisted.count()
)

print(
    "Persisted valid entity rows:",
    entity_persisted.count()
)

print(
    "Persisted rejected entity rows:",
    rejected_persisted.count()
)

print("\n=== PRIMARY KEY CHECKS ===")

print(
    "Duplicate pipeline run IDs:",
    pipeline_duplicate_ids
)

print(
    "Duplicate valid entity run IDs:",
    entity_duplicate_ids
)

print(
    "Duplicate rejected entity run IDs:",
    rejected_duplicate_ids
)

print(
    "Null pipeline run IDs:",
    pipeline_null_ids
)

print(
    "Null valid entity run IDs:",
    entity_null_ids
)

print(
    "Null rejected entity run IDs:",
    rejected_null_ids
)

print("\n=== RELATIONSHIP CHECKS ===")

print(
    "Valid entity parent pipeline orphans:",
    entity_parent_orphans
)

print(
    "Entity IDs present in both valid and rejected tables:",
    valid_rejected_overlap
)

print("\n=== CURRENT SOURCE CLASSIFICATION ===")

print(
    "Current source entity rows:",
    source_entity_rows
)

print(
    "Current valid rows:",
    current_valid_rows
)

print(
    "Current rejected rows:",
    current_rejected_rows
)

print(
    "Classification reconciliation difference:",
    classification_difference
)


# =====================================================
# 10. FINAL HARD FAILURE RESULT
# =====================================================

hard_failure_count = (
    pipeline_duplicate_ids
    + entity_duplicate_ids
    + rejected_duplicate_ids
    + pipeline_null_ids
    + entity_null_ids
    + rejected_null_ids
    + entity_parent_orphans
    + valid_rejected_overlap
    + abs(classification_difference)
)

print("\n=== FINAL GOLD MONITORING RESULT ===")

print(
    "Hard persistence failure count:",
    hard_failure_count
)

if hard_failure_count == 0:
    print(
        "PASS: Gold monitoring tables persisted successfully."
    )
else:
    print(
        "FAIL: Gold monitoring persistence requires investigation."
    )