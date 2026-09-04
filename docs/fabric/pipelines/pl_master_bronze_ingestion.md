# Bronze Master Ingestion Pipeline

**Pipeline:** `pl_master_bronze_ingestion`

## Purpose

The Bronze master pipeline provides metadata-driven orchestration for ingestion across BritMart's heterogeneous source systems.

Rather than maintaining a separate orchestration flow for every entity, the pipeline reads active entity configuration from the control framework and dynamically routes each entity to the appropriate source-specific child pipeline.

## Pipeline Parameters

| Parameter | Purpose |
|---|---|
| `p_source_system_code` | Identifies the source system to process |
| `p_requested_load_type` | Specifies the requested load type |
| `p_window_start_utc` | Start of the requested processing window |
| `p_window_end_utc` | End of the requested processing window |

## Execution Flow

```text
SET_Pipeline_Run_ID
        │
        ▼
SCR_Pipeline_Run_Start
        │
        ▼
LKP_Active_Entities
        │
        ▼
FE_Process_Entities
        │
        ▼
SW_Route_Source_System
        │
        ├── SUPPLIER_API
        │      └── pl_ingest_supplier_api
        │
        ├── WAREHOUSE_SQL
        │      └── pl_ingest_warehouse_sql_bronze
        │
        ├── STORE_POS
        │      └── pl_ingest_sharepoint_pos_bronze
        │
        ├── ECOMMERCE_S3
        │      └── pl_ingest_ecommerce_s3_bronze
        │
        └── LOGISTICS_STREAM
               └── pl_ingest_logistics_stream_bronze

```

## Metadata-Driven Processing

LKP_Active_Entities retrieves active entity configuration from the control framework.

FE_Process_Entities iterates through the returned metadata. The current metadata record supplies runtime configuration to the relevant child pipeline.

This separates:

What should run — control metadata
When and in what sequence it should run — master orchestration
How the source is ingested — source-specific child pipeline
Audit Framework

Execution is audited at two levels:

Pipeline Level

ctl.pipeline_run

Captures the overall pipeline execution, including status, timestamps and execution context.

Entity Level

ctl.entity_run

Captures execution information for individual entities processed by the master pipeline.

This provides traceability from the master pipeline down to individual ingestion operations.

Failure Handling

Successful execution updates the pipeline audit through:

SCR_Pipeline_Run_Success

Failures are routed to:

SCR_Pipeline_Run_Failed

Entity-level execution information is retained separately, enabling failed entities to be identified without relying solely on the overall pipeline status.

Concurrency Strategy

Entity processing is currently configured sequentially with a batch count of 1.

This is intentional for the portfolio environment because Microsoft Fabric trial capacity is constrained. The metadata framework can support a different concurrency strategy when deployed to appropriately sized production capacity.

Design Benefits
Centralised orchestration
Metadata-driven extensibility
Reduced pipeline duplication
Source-specific separation of concerns
Incremental processing support
Pipeline and entity-level auditability
Consistent failure handling
Easier onboarding of additional entities
