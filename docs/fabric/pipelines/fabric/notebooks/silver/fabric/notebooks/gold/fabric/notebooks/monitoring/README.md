# Data Quality & Operational Monitoring

BritMart includes engineering controls for data quality, reconciliation, pipeline auditing and operational observability.

The objective is not only to process data successfully, but also to provide evidence that data moved through the platform completely, consistently and with traceable execution history.

## Validation Framework

Validation is implemented across multiple layers:

```text
Source / Bronze
      │
      ▼
Silver Transformation
      │
      ├── Schema & datatype validation
      ├── Business-key validation
      ├── Duplicate detection
      ├── Referential integrity
      └── Source-equivalent reconciliation
      │
      ▼
Gold Transformation
      │
      ├── Primary-key validation
      ├── Foreign-key validation
      ├── Relationship integrity
      └── Silver-to-Gold reconciliation
      │
      ▼
Operational Monitoring
```

## Current Validation Evidence

| Control | Result |
|---|---:|
| Silver data-quality checks | 28 / 28 passed |
| Silver reconciliation checks | 16 / 16 passed |
| Gold relationship checks | 23 / 23 passed |
| Silver-to-Gold reconciliation | 6 / 6 passed |

These results represent the currently validated project dataset and are used as engineering evidence rather than as static business KPIs.

## Pipeline Audit Model

Execution metadata is captured through two principal control tables:

### `ctl.pipeline_run`

Stores pipeline-level execution information such as:

- pipeline run identifier
- trigger context
- requested load type
- processing window
- start and completion timestamps
- execution status
- duration

### `ctl.entity_run`

Stores entity-level execution information including:

- entity run identifier
- parent pipeline run identifier
- source system
- entity
- execution status
- source and destination counts
- bytes processed
- execution timestamps
- error information where applicable

This provides traceability from platform-level execution to individual ingestion operations.

## Gold Monitoring Tables

Operational audit data is curated into:

- `gold_pipeline_run`
- `gold_entity_run`
- `gold_entity_run_rejected`

The monitoring transformation validates audit relationships before exposing records to the curated monitoring layer.

## Audit Relationship Validation

Historical audit data contained entity-run records whose referenced parent pipeline-run records were unavailable.

Rather than fabricating parent records or deleting the anomalous entity records, the monitoring process classifies them explicitly.

```text
ctl.entity_run
      │
      ├── Valid parent relationship
      │          │
      │          ▼
      │    gold_entity_run
      │
      └── Missing parent relationship
                 │
                 ▼
        gold_entity_run_rejected
```

Current classification:

| Audit classification | Rows |
|---|---:|
| Source entity-run records | 152 |
| Valid curated records | 147 |
| Quarantined records | 5 |
| Classification difference | 0 |

The five quarantined records are retained for traceability rather than silently removed.

## Monitoring Persistence Validation

Current monitoring persistence checks confirm:

- no duplicate pipeline-run identifiers
- no duplicate valid entity-run identifiers
- no duplicate rejected entity-run identifiers
- no null audit identifiers
- no parent-pipeline orphans in the curated valid entity dataset
- no overlap between valid and rejected entity classifications
- complete reconciliation between source, valid and rejected records

**Hard persistence failures: 0**

## Historical Execution Visibility

Historical audit records may retain statuses such as `RUNNING` where previous executions did not reach a final audit update.

These records are preserved rather than rewritten to create artificially clean operational history.

The monitoring layer is designed to surface these conditions so they can be investigated.

## Engineering Principles

The monitoring framework follows several principles:

- preserve source audit history
- quarantine invalid relationships rather than fabricate data
- reconcile classifications back to source
- expose pipeline and entity-level execution status
- retain failure information for investigation
- validate monitoring data before reporting it
- keep operational monitoring separate from business facts
