# Metadata-Driven Control Framework

BritMart uses a metadata-driven ingestion framework to control how source entities are discovered, routed, processed and audited.

The framework separates orchestration logic from source-specific configuration, allowing ingestion behaviour to be changed through metadata rather than by duplicating pipeline logic.

## Control Tables

| Table | Purpose |
|---|---|
| `ctl.source_system` | Registers source systems and their active status |
| `ctl.ingestion_config` | Defines entity-level ingestion configuration and routing |
| `ctl.watermark_tracker` | Stores incremental processing state |
| `ctl.pipeline_run` | Captures pipeline-level execution audit information |
| `ctl.entity_run` | Captures entity-level execution audit information |
| `ctl.processed_source_object` | Tracks processed source objects for idempotency |
| `ctl.file_batch_tracker` | Tracks file-based ingestion batches |

## Orchestration Pattern

The Bronze master pipeline follows the pattern:

`Start → Load Metadata → Iterate Entities → Route Source System → Execute Child Pipeline → Audit Result`

The master pipeline determines **what should run**, while child pipelines implement **how each source type is ingested**.

## Source-Specific Child Pipelines

- `pl_ingest_supplier_api`
- `pl_ingest_warehouse_sql_bronze`
- `pl_ingest_sharepoint_pos_bronze`
- `pl_ingest_ecommerce_s3_bronze`
- `pl_ingest_logistics_stream_bronze`

## Engineering Objectives

The control framework is designed to support:

- metadata-driven orchestration
- incremental ingestion
- watermark management
- source-specific routing
- execution auditing
- failure traceability
- idempotent processing
- extensibility for additional sources and entities
