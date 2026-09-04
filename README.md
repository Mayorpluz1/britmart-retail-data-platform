# BritMart Retail Data Platform

> **Production-oriented, multi-source retail data platform built with Microsoft Fabric, PySpark, Delta Lake and Power BI, demonstrating metadata-driven ingestion, incremental processing, data quality, reconciliation and operational observability.**

> **Project status:** Work in progress — core Bronze, Silver and Gold engineering layers are implemented and validated. Final end-to-end orchestration, documentation and deployment evidence are being completed.

---

## Overview

BritMart is a portfolio reference implementation of an end-to-end data platform for a fictional UK retailer operating across physical stores, e-commerce, warehousing, procurement and logistics.

The platform integrates heterogeneous operational data from **REST APIs, SQL Server, AWS S3, SharePoint and Azure Blob Storage** into a governed Microsoft Fabric Lakehouse architecture.

The objective is not simply to move data between systems. The project demonstrates engineering patterns expected in production data platforms:

- Metadata-driven ingestion
- Parameterised orchestration
- Full and incremental processing
- Watermark-based ingestion
- Idempotent Delta Lake processing
- Schema enforcement and controlled schema drift
- Deduplication and business-key validation
- Data quality and referential-integrity controls
- Cross-layer reconciliation
- Audit logging and operational monitoring
- Quarantine of invalid monitoring records
- Dimensional modelling
- Semantic-model refresh
- Failure diagnosis and recovery

---

## Platform Architecture

```text
┌──────────────────────────────── SOURCE SYSTEMS ────────────────────────────────┐
│                                                                                │
│  Supplier API       Warehouse SQL       Store POS       E-commerce   Logistics │
│  REST / FastAPI     SQL Server          SharePoint      AWS S3       Azure Blob │
│                                                                                │
└──────────┬────────────────┬─────────────────┬──────────────┬─────────────┬───────┘
           │                │                 │              │             │
           └────────────────┴──────────┬──────┴──────────────┴─────────────┘
                                      │
                                      ▼
                         METADATA-DRIVEN INGESTION
                    Control Tables • Configuration • Watermarks
                         Audit Logging • Run Tracking
                                      │
                                      ▼
                             ┌─────────────────┐
                             │     BRONZE      │
                             │   Raw Landing   │
                             └────────┬────────┘
                                      │
                                      ▼
                             ┌─────────────────┐
                             │     SILVER      │
                             │ Clean • Conform │
                             │ Validate • MERGE│
                             └────────┬────────┘
                                      │
                           Data Quality + Reconciliation
                                      │
                                      ▼
                             ┌─────────────────┐
                             │      GOLD       │
                             │ Dimensions/Facts│
                             └────────┬────────┘
                                      │
                            Gold Quality Controls
                                      │
                         ┌────────────┴────────────┐
                         ▼                         ▼
                 Semantic Model           Monitoring Mart
                         │
                         ▼
                      Power BI
```

> A detailed architecture diagram and component-level documentation will be maintained under `docs/architecture/`.

---

## Source Systems

| Business Domain | Source Technology | Representative Data |
|---|---|---|
| Supplier & Procurement | REST API / FastAPI | Suppliers, purchase orders, shipments, supplier performance |
| Warehouse | SQL Server | Distribution centres, goods receipts, inventory movements |
| Store Sales | SharePoint | POS transaction files |
| E-commerce | AWS S3 | Orders, payments, fulfilment events |
| Logistics | Azure Blob Storage | Micro-batch logistics event files |

**Note:** Logistics is intentionally described as a **micro-batch/file-based event feed**, rather than true streaming.

---

## Metadata-Driven Ingestion

The ingestion framework separates **configuration from execution logic**.

Control metadata determines:

- Source system
- Entity
- Child ingestion pipeline
- Load type
- Execution sequence
- Source path
- Pagination configuration
- Watermark column
- Tie-breaker column
- Primary key
- Bronze destination
- Active/inactive status

A master pipeline reads this metadata and dynamically routes each entity to the appropriate source-specific ingestion pipeline.

```text
ctl.source_system
        │
ctl.ingestion_config
        │
        ▼
pl_master_bronze_ingestion
        │
        ├── Supplier API
        ├── Warehouse SQL
        ├── Store POS
        ├── E-commerce S3
        └── Logistics event files
```

This reduces duplicated orchestration logic and allows new entities to be onboarded primarily through configuration.

---

## Medallion Architecture

### Bronze — Raw Ingestion

Bronze preserves source-aligned data with minimal transformation.

Responsibilities include:

- Source extraction
- Parameterised ingestion
- Full/incremental load handling
- API pagination
- File ingestion
- Watermark tracking
- Run auditing
- Raw data preservation

### Silver — Validated & Conformed

Silver transforms raw data into trusted analytical datasets.

Key controls include:

- Explicit type casting
- Schema enforcement
- Standardisation
- Business-key validation
- Deduplication
- Referential-integrity checks
- Technical audit columns
- Record hashing
- Delta `MERGE`
- Quarantine where appropriate
- Idempotent reprocessing

### Gold — Analytics Model

Gold provides business-facing dimensional structures optimised for reporting and semantic modelling.

Implemented dimensions include:

- Date
- Supplier
- Distribution Centre
- POS Store
- E-commerce Store
- POS Product
- E-commerce Product
- Customer
- Sales Channel

Implemented facts include:

- Sales
- Purchase Orders
- Supplier Shipments
- Logistics Events
- Supplier Performance

---

## Data Quality & Reconciliation

Quality controls are implemented as executable engineering checks rather than relying solely on dashboard-level validation.

Current validated results:

| Validation Layer | Result |
|---|---:|
| Silver data-quality checks | **28 / 28 passed** |
| Silver reconciliation | **16 / 16 passed** |
| Gold relationship checks | **23 / 23 passed** |
| Silver → Gold reconciliation | **6 / 6 passed** |
| Gold monitoring persistence failures | **0** |

Checks cover areas including:

- Null business keys
- Duplicate primary keys
- Referential integrity
- Source/target row reconciliation
- Invalid timestamps
- Invalid counts
- Fact/dimension relationships
- Cross-domain relationship validation

---

## Operational Monitoring

Pipeline execution is captured at two levels:

```text
Pipeline Run
    │
    ├── Entity Run
    ├── Entity Run
    └── Entity Run
```

The Gold monitoring mart currently classifies **152 historical entity-run audit records**:

```text
152 source audit records
├── 147 valid monitoring records
└──   5 quarantined records
```

The five records with missing parent pipeline runs are preserved rather than silently deleted or assigned fabricated relationships.

They are isolated with the rejection reason:

`PARENT_PIPELINE_RUN_NOT_FOUND`

This keeps the curated monitoring model referentially valid while retaining evidence of historical audit anomalies for investigation.

---

## Idempotency & Incremental Processing

The platform is designed so that rerunning the same processing window does not create duplicate business records.

Key patterns include:

- Watermarks for incremental extraction
- Stable business keys
- Deterministic deduplication
- Record hashes for change detection
- Delta Lake `MERGE`
- Source-to-target reconciliation
- Explicit processing windows

End-to-end idempotency validation is part of the final platform verification.

---

## Data Modelling Decisions

Some source domains intentionally remain separate.

For example, POS and e-commerce stores are **not artificially merged into a single enterprise store dimension** because no authoritative cross-system mapping exists.

The same principle applies to product identities across POS and e-commerce.

This is deliberate: the platform avoids manufacturing master-data relationships that cannot be supported by source-system evidence.

Similarly, operational relationships between certain fact datasets are validated through data-quality controls rather than introducing inappropriate fact-to-fact relationships into the semantic model.

---

## Technology Stack

| Area | Technology |
|---|---|
| Data Platform | Microsoft Fabric |
| Distributed Processing | Apache Spark / PySpark |
| Storage | OneLake / Delta Lake |
| Transformation | PySpark, Spark SQL, SQL |
| Orchestration | Microsoft Fabric Data Factory |
| Supplier System | FastAPI / PostgreSQL |
| Cloud Sources | AWS S3, Azure Blob Storage |
| File Source | SharePoint |
| Warehouse Source | SQL Server |
| Analytics | Power BI |
| Version Control | Git / GitHub |

---

## End-to-End Orchestration

The platform-level orchestration coordinates source ingestion, transformation and reporting refresh:

```text
BRONZE_SUPPLIER_API
        ↓
BRONZE_WAREHOUSE_SQL
        ↓
BRONZE_STORE_POS
        ↓
BRONZE_ECOMMERCE_S3
        ↓
BRONZE_LOGISTICS
        ↓
SILVER_PROCESSING
        ↓
GOLD_PROCESSING
        ↓
REFRESH_SEMANTIC_MODEL
```

The orchestration is intentionally sequential in the current reference implementation to provide predictable resource utilisation within the available Fabric development capacity.

---

## Engineering Principles Demonstrated

This project focuses on engineering decisions rather than maximising the number of technologies used.

Core principles include:

1. **Configuration over duplication** — ingestion behaviour is driven through metadata.
2. **Idempotency by design** — reprocessing should not duplicate business records.
3. **Data quality as code** — quality controls execute as part of the engineering workflow.
4. **Reconciliation across boundaries** — important transformations are quantitatively validated.
5. **Preserve evidence** — invalid audit records are quarantined rather than silently discarded.
6. **Do not fabricate relationships** — master-data limitations remain explicit.
7. **Observability is part of the platform** — pipeline and entity execution are modelled for operational analysis.
8. **Security by design** — credentials and secrets are excluded from source control.

---

## Repository Structure

```text
britmart-retail-data-platform/
├── docs/                  # Architecture, design decisions and engineering documentation
├── fabric/
│   ├── pipelines/         # Pipeline definitions and documentation
│   ├── notebooks/         # Silver, Gold and monitoring transformations
│   └── control-framework/ # Metadata and audit framework
├── sql/                   # Control-table and validation SQL
├── tests/                 # Data-quality and reconciliation tests
├── power-bi/              # Semantic-model documentation and report evidence
├── sample-data/           # Sanitised representative data only
└── README.md
```

The repository will be populated progressively as final platform validation and documentation are completed.

---

## Security

No production credentials, API keys, access tokens, passwords or private connection strings are stored in this repository.

Configuration examples use placeholders or environment variables. Any representative datasets published here are synthetic or sanitised.

---

## Project Status

**Active development / final validation**

Completed:

- Multi-source Bronze ingestion framework
- Metadata-driven orchestration
- Incremental processing patterns
- Silver transformation layer
- Silver data-quality framework
- Silver reconciliation
- Gold dimensional model
- Gold data-quality validation
- Operational monitoring mart
- Power BI semantic/reporting layer

In progress:

- Final end-to-end orchestration validation
- Repeat-window idempotency proof
- CI/CD implementation and documentation
- Architecture and data-model diagrams
- Repository documentation and implementation evidence

---

## Disclaimer

BritMart is a **fictional retail organisation** created as a portfolio/reference implementation. The architecture and datasets are designed to demonstrate production-oriented data engineering patterns and do not represent a live BritMart business or client deployment.
