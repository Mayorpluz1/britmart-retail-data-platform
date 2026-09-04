# Silver Transformation Layer

The Silver layer transforms raw Bronze data into validated, standardised and analytics-ready Delta tables.

Processing is implemented primarily with PySpark and Delta Lake and follows consistent engineering patterns across the different BritMart source systems.

## Processing Flow

```text
Bronze
  │
  ▼
Read Source Data
  │
  ▼
Schema Enforcement & Type Casting
  │
  ▼
Standardisation
  │
  ▼
Business-Key Validation
  │
  ▼
Deduplication
  │
  ▼
Referential Integrity Checks
  │
  ▼
Record Hash Generation
  │
  ▼
Delta MERGE
  │
  ▼
Silver
```

## Silver Notebooks

| Notebook | Responsibility |
|---|---|
| `10_nb_supplier_master` | Supplier master transformation |
| `11_nb_procurement` | Purchase order processing |
| `12_nb_supplier_shipments` | Supplier shipment processing |
| `14_nb_supplier_performance` | Supplier performance events |
| `20_nb_store_pos` | Store POS transformation |
| `21_nb_ecommerce` | E-commerce transformation |
| `30_nb_warehouse` | Warehouse and inventory processing |
| `31_nb_logistics` | Logistics event processing |
| `90_nb_data_quality` | Silver data-quality validation |
| `91_nb_silver_reconciliation` | Source-equivalent to Silver reconciliation |

`13_nb_goods_receipts` is not part of the active processing path because the Supplier API does not currently expose the required goods-receipts endpoint.

## Incremental Processing

Where supported by the source, the platform processes new and changed records incrementally rather than rebuilding the complete dataset for every execution.

Delta MERGE operations provide idempotent upsert behaviour using business keys and record-level change detection.

Record hashes are used where appropriate to identify meaningful changes while excluding volatile processing metadata.

## Data Quality

Silver processing includes controls for:

- null business keys
- duplicate business keys
- schema and datatype consistency
- referential integrity
- source-to-target row reconciliation
- orphan detection
- invalid relationships

Data-quality checks are treated as part of the engineering pipeline rather than only as downstream reporting checks.

## Reconciliation Evidence

Current Silver validation:

| Validation | Result |
|---|---:|
| Silver data-quality checks | 28 / 28 passed |
| Silver reconciliation checks | 16 / 16 passed |

These checks provide evidence that the transformed Silver datasets satisfy the implemented technical and relationship controls.

## Logistics Processing

Logistics events are implemented as a **micro-batch/file-based event feed** sourced from Azure Blob Storage.

The Silver process dynamically discovers valid logistics event files, excludes backup/non-production files, combines historical and incremental batches, validates relationships and persists the resulting records to Delta.

This implementation should not be interpreted as a true real-time streaming architecture.

## Engineering Principles

The Silver layer is designed around:

- deterministic transformations
- repeatable processing
- idempotency
- explicit data-quality controls
- traceability
- schema consistency
- separation of ingestion and transformation concerns
