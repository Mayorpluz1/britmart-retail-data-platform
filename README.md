# BritMart Retail Data Platform

> **Production-oriented Microsoft Fabric retail data engineering
reference implementation**

> **Project status:** Complete --- implemented and validated
end-to-end across multi-source ingestion, Bronze, Silver, Gold, data
quality, reconciliation, operational monitoring, semantic modelling and
Power BI reporting.

BritMart Retail Data Platform is an end-to-end data engineering
reference implementation built around **Microsoft Fabric, PySpark,
Delta Lake, SQL and Power BI**.

The project models a fictional UK omnichannel retailer operating
physical stores and e-commerce channels, supported by distribution
centres, suppliers, procurement and logistics operations.

The engineering objective was not simply to move data and build
dashboards. The platform demonstrates how heterogeneous operational
systems can be integrated into a governed analytical architecture with
**metadata-driven orchestration, incremental processing,
idempotency, data-quality controls, reconciliation, auditability,
quarantine handling and operational observability**.

---

## Table of Contents

1. [Architecture](#architecture)

2. [Source Systems](#source-systems)

3. [Supplier REST API Source
System](#supplier-rest-api-source-system)

4. [Metadata-Driven Ingestion](#metadata-driven-ingestion)

5. [Control Framework](#control-framework)

6. [Bronze Layer](#bronze-layer)

7. [Silver Layer](#silver-layer)

8. [Data Quality](#data-quality)

9. [Reconciliation](#reconciliation)

10. [Gold Analytical Layer](#gold-analytical-layer)

11. [Operational Monitoring](#operational-monitoring)

12. [End-to-End Orchestration](#end-to-end-orchestration)

13. [Semantic Model](#semantic-model)

14. [Power BI Analytics](#power-bi-analytics)

15. [Git Engineering Workflow](#git-engineering-workflow)

16. [Key Engineering Decisions](#key-engineering-decisions)

17. [Technology Stack](#technology-stack)

18. [Repository Structure](#repository-structure)

19. [Security](#security)

20. [Portfolio Scope](#portfolio-scope)

---

# Architecture

![BritMart Retail Data Platform
Architecture](docs/evidence/01-platform-architecture.png)

BritMart follows a Medallion-style architecture with clear separation
between ingestion, raw persistence, conformance, analytical modelling
and consumption.

```text

Operational Source Systems

        ↓

Metadata-Driven Ingestion

        ↓

Bronze --- Raw / Source-Aligned

        ↓

Silver --- Validated / Conformed

        ↓

Gold --- Dimensional / Analytical

        ↓

Semantic Model

        ↓

Power BI

```

The architecture separates the following responsibilities:

- source-system integration

- orchestration

- raw-data persistence

- transformation and conformance

- data-quality enforcement

- reconciliation

- dimensional modelling

- operational monitoring

- semantic modelling

- business analytics

### Why this architecture?

The key design principle is **separation of concerns**.

Ingestion logic should not contain business transformation logic. Silver
transformation should not depend on Power BI. Operational audit data
should not be overwritten merely to make monitoring easier.

This separation improves maintainability, traceability, failure
isolation and the ability to evolve individual platform components
independently.

---

# Source Systems

BritMart integrates five heterogeneous source families.

| Business Domain | Source Technology | Representative Data |

|---|---|---|

| Supplier & Procurement | FastAPI + PostgreSQL | Suppliers, purchase
orders, shipments, supplier performance |

| Warehouse | SQL Server | Distribution centres, goods receipts,
inventory movements |

| Store Sales | SharePoint files | POS transactions, transaction
lines, payments |

| E-commerce | AWS S3 | Orders, order lines, payments, fulfilment
events |

| Logistics | Azure Blob Storage | Logistics event files |

The logistics source is intentionally described as a
**micro-batch/file-based event feed**. It is not presented as
true event streaming.

### Engineering significance

The sources deliberately require different ingestion patterns:

- REST API pagination and authentication

- relational extraction

- object-storage ingestion

- file processing

- micro-batch event-file ingestion

Rather than forcing every source through identical extraction logic,
BritMart uses **source-specific ingestion mechanisms governed by
common metadata and audit standards**.

---

# Supplier REST API Source System

A separate operational supplier and procurement application was
developed to provide a realistic upstream REST API.

The source application uses:

- FastAPI

- PostgreSQL

- Python

- API-key authentication

- pagination

- relational operational models

- generated procurement and supplier data

It exposes implemented API domains for:

- suppliers

- purchase orders

- shipments

- supplier performance events

- supplier performance scorecards

![Supplier API Swagger](docs/evidence/04-supplier-api-swagger.png)

The source application remains separate from the analytical platform,
reflecting the architectural boundary that normally exists between an
operational application and an enterprise data platform.

The integration path is:

```text

PostgreSQL

    ↓

FastAPI

    ↓

Authenticated REST API

    ↓

Microsoft Fabric Pipeline

    ↓

Bronze JSON

    ↓

Silver Delta Tables

```

![Supplier API Bronze
Ingestion](docs/evidence/05-supplier-api-bronze-ingestion.png)

**Separate source-system repository:**  

[BritMart Supplier
API](https://github.com/Mayorpluz1/britmart-supplier-api)

---

# Metadata-Driven Ingestion

The Bronze ingestion framework is **metadata-driven**.

Instead of building independent orchestration logic for every entity,
the master pipeline reads configuration and determines what should
execute.

![Metadata-Driven Master
Pipeline](docs/evidence/02-master-metadata-pipeline.png)

Conceptually:

```text

SET_Pipeline_Run_ID

        ↓

SCR_Pipeline_Run_Start

        ↓

LKP_Active_Entities

        ↓

FE_Process_Entities

        ↓

SW_Route_Source_System

        ↓

Source-Specific Child Pipeline

        ↓

Audit Success / Failure

```

Metadata controls attributes such as:

- source system

- entity code

- entity name

- child pipeline

- load type

- load sequence

- source path

- pagination configuration

- watermark column

- tie-breaker column

- primary/business keys

- Bronze destination

- file format

- active status

The architectural principle is:

> **Metadata defines what should happen; pipelines define how it
happens.**

The master pipeline owns orchestration and routing. Child pipelines own
source-specific extraction behaviour.

### Why this matters

Without metadata-driven orchestration, adding another entity can require
cloning and maintaining another pipeline.

With metadata-driven orchestration, many changes become configuration
changes rather than orchestration redesigns.

That reduces duplicated logic and improves scalability and
maintainability.

---

# Control Framework

Operational configuration and audit state are maintained in the Fabric
Warehouse.

![Control Framework](docs/evidence/03-control-framework.png)

The core framework consists of:

```text

ctl.source_system

ctl.ingestion_config

ctl.watermark_tracker

ctl.processed_source_object

ctl.pipeline_run

ctl.entity_run

```

### `ctl.source_system`

Registers operational source systems and provides stable source-system
identifiers.

### `ctl.ingestion_config`

Contains entity-level ingestion metadata used by the master
orchestration pipeline.

### `ctl.watermark_tracker`

Maintains incremental extraction state so subsequent runs can process
only new or changed data where supported.

### `ctl.processed_source_object`

Tracks processed file/source objects and supports idempotent file
ingestion.

### `ctl.pipeline_run`

Captures pipeline-level execution telemetry.

### `ctl.entity_run`

Captures entity-level execution telemetry including execution state and
ingestion metrics.

---

# Bronze Layer

Bronze preserves data in a source-aligned form before business
transformation.

Its responsibilities include:

- source fidelity

- traceability

- replayability

- incremental ingestion

- idempotency

- ingestion metadata

- source-specific extraction

- audit capture

Bronze is intentionally not the place for extensive business
transformation.

### Why preserve raw/source-aligned data?

If downstream transformation logic changes, the platform should ideally
be able to reprocess retained source data without repeatedly extracting
everything from the operational system.

Bronze therefore acts as the boundary between **source
ingestion** and **analytical transformation**.

---

# Silver Layer

Silver converts source-aligned Bronze data into validated and conformed
Delta tables.

Core processing includes:

- explicit schema handling

- data-type casting

- standardisation

- business-key validation

- duplicate handling

- referential-integrity validation

- record hashing

- audit columns

- quarantine handling

- Delta MERGE operations

- incremental and idempotent processing

Representative technical columns include:

```text

_source_system

_processed_at_utc

_record_hash

```

Silver processing is separated by business/source domain rather than
implemented as one monolithic transformation notebook.

### Why Delta MERGE?

Incremental pipelines must distinguish inserts from updates while
remaining safe to rerun.

Using deterministic keys together with Delta MERGE allows the curated
layer to update existing records and insert new records without blindly
appending duplicates.

### Why record hashes?

Record hashes provide an efficient mechanism for determining whether the
relevant source attributes of an existing record have changed.

---

# Data Quality

Data quality is implemented as executable engineering logic rather than
relying solely on dashboard inspection.

The framework validates areas such as:

- duplicate keys

- null business keys

- referential integrity

- cross-table consistency

- invalid identifiers

- transformation integrity

Final validation:

```text

28 / 28 checks PASS

```

![Silver Data Quality](docs/evidence/06-silver-data-quality.png)

A hard validation failure is treated as an engineering condition
requiring investigation rather than being silently ignored.

---

# Reconciliation

Data quality and reconciliation solve related but different problems.

The reconciliation framework compares expected/source-equivalent counts
with Silver outputs.

Final validation:

```text

16 / 16 reconciliation checks PASS

```

![Silver Reconciliation](docs/evidence/07-silver-reconciliation.png)

### Data quality vs reconciliation

**Data quality asks:**

> Are the records valid?

**Reconciliation asks:**

> Did the expected records arrive and survive processing?

A dataset can contain individually valid records while still being
incomplete. This is why reconciliation is implemented separately from
row-level validation.

---

# Gold Analytical Layer

Gold provides business-ready dimensional models for reporting and
analytics.

### Dimensions

- Date

- Supplier

- Distribution Centre

- Store

- Product

- Customer

- Sales Channel

### Facts

- Sales

- Purchase Orders

- Supplier Shipments

- Logistics Events

- Supplier Performance

The Gold layer is organised around **business processes and
analytical grain**, rather than exposing Silver tables directly to
report authors.

### Why dimensional modelling?

The objective is to create a predictable analytical interface.

Facts represent measurable business processes, while dimensions provide
reusable descriptive context.

This improves semantic consistency and reduces the amount of
transformation logic required inside individual reports.

---

# Operational Monitoring

A production-oriented data platform must make its own behaviour
observable.

BritMart therefore models pipeline and entity execution telemetry as
analytical data.

The original source `run_status` is preserved.

A separate `monitoring_status` is derived so historical executions
that remained in `RUNNING` state beyond the accepted threshold can be
classified as `STALE` without rewriting the original audit record.

## Pipeline Monitoring

Validated state:

```text

109 pipeline runs

├── 82 SUCCESS

├── 18 FAILED

└──  9 STALE

```

## Entity Monitoring

```text

204 source entity-run records

│

├── 197 valid Gold monitoring records

│   ├── 139 SUCCESS

│   ├──  54 STALE

│   ├──   3 SKIPPED

│   └──   1 FAILED

│

└── 7 quarantined records

```

Entity executions whose parent pipeline audit record cannot be resolved
are quarantined rather than assigned fabricated relationships.

The rejection reason is retained as:

```text

PARENT_PIPELINE_RUN_NOT_FOUND

```

Final persistence validation:

```text

Duplicate pipeline run IDs:                 0

Duplicate valid entity run IDs:             0

Duplicate rejected entity run IDs:          0

Valid entity parent pipeline orphans:       0

Valid/rejected entity overlap:              0

Classification reconciliation difference:  0

Hard persistence failure count:             0

PASS

```

![Gold Monitoring
Validation](docs/evidence/08-gold-monitoring-validation.png)

### Why preserve `run_status` and derive `monitoring_status`?

Because the audit table represents historical source truth.

If an execution was originally recorded as `RUNNING`, rewriting it as
`STALE` would mutate that historical evidence.

Instead:

```text

run_status        = RUNNING

monitoring_status = STALE

```

This gives operations a useful interpretation while preserving the
original audit record.

### Why quarantine orphan monitoring records?

Creating a fake parent identifier would make the data look relationally
correct while corrupting lineage.

The safer pattern is:

```text

valid relationship

        → curated monitoring fact

invalid relationship

        → quarantine + rejection reason

```

This makes the defect visible and recoverable.

---

# End-to-End Orchestration

The final orchestration pipeline coordinates the complete analytical
workflow.

![End-to-End Pipeline](docs/evidence/09-end-to-end-pipeline.png)

Execution follows:

```text

Supplier API Bronze

        ↓

Warehouse SQL Bronze

        ↓

Store POS Bronze

        ↓

E-commerce S3 Bronze

        ↓

Logistics Bronze

        ↓

Silver Processing

        ↓

Gold Processing

        ↓

Semantic Model Refresh

```

The orchestration maintains explicit dependencies between ingestion,
conformance, analytical modelling and downstream consumption.

Sequential execution was also appropriate for the constrained Fabric
trial environment and provides predictable dependency ordering.

---

# Semantic Model

The Gold analytical layer is exposed through a governed semantic model.

![Semantic Model](docs/evidence/10-semantic-model.png)

The model uses legitimate dimensional relationships and business grain
rather than introducing arbitrary fact-to-fact relationships merely to
make filtering convenient.

It supports analysis across:

- platform operations

- executive performance

- sales

- procurement

- supplier performance

- logistics

### Modelling principle

Where two facts share business context, the preferred pattern is
generally to relate them through appropriate dimensions rather than
directly connecting fact tables without a valid grain relationship.

---

# Power BI Analytics

Four focused analytical views represent different consumption
requirements.

## 1. Data Platform Monitoring

![Data Platform Monitoring](docs/evidence/11-monitoring-dashboard.png)

Provides operational visibility into:

- pipeline success rate

- failed pipeline runs

- stale executions

- average pipeline duration

- entity execution health

- ingestion volume

- pipeline status over time

- entity-level diagnostics

The monitoring page demonstrates that observability is treated as part
of the platform rather than as an afterthought.

---

## 2. Executive Overview

![Executive Dashboard](docs/evidence/12-executive-dashboard.png)

Provides a consolidated executive view across retail and supply-chain
performance, including:

- net sales

- transaction volume

- sales quantity

- purchase-order value

- shipment volume

- supplier pass rate

- sales trend

- supplier performance

- delivery performance

---

## 3. Sales Analytics

![Sales Dashboard](docs/evidence/13-sales-dashboard.png)

Provides deeper analysis of retail sales performance across relevant
business dimensions and sales channels.

---

## 4. Procurement & Supplier Performance

The procurement analytical domain combines:

- purchase-order activity

- supplier shipments

- delivery performance

- supplier-performance events

- supplier-level KPIs

This allows procurement performance to be analysed from order creation
through supplier fulfilment and performance measurement.

---

# Git Engineering Workflow

Git and GitHub are used to isolate engineering changes and maintain
traceable history.

The demonstrated workflow is:

```text

main

  ↓

feature branch

  ↓

implementation

  ↓

validation

  ↓

git add

  ↓

commit

  ↓

push

  ↓

Pull Request

  ↓

diff / security review

  ↓

merge

  ↓

synchronise local main

  ↓

branch cleanup

```

The Gold monitoring capability was developed on:

```text

feature/gold-monitoring

```

and merged into `main` through Pull Request #1 after implementation
and validation.

![GitHub Pull Request](docs/evidence/15-github-pull-request.png)

The Pull Request provides evidence of:

- isolated feature development

- meaningful commit history

- change review

- validation evidence

- controlled integration into `main`

### Why use Git if Fabric already stores artefacts?

Git solves a different problem.

It provides version history, change isolation, reviewability and a
controlled mechanism for integrating engineering changes.

A feature branch allows development without directly changing the stable
branch.

---

# Key Engineering Decisions

## 1. Metadata over duplicated orchestration

**Decision:** Drive entity execution from configuration.

**Reason:** Avoid duplicating orchestration logic as the number
of entities increases.

**Benefit:** Scalability and maintainability.

---

## 2. Source-specific ingestion with common governance

**Decision:** Allow REST, SQL and file sources to use different
extraction implementations while sharing metadata and audit standards.

**Reason:** Different technologies require different extraction
behaviour.

**Benefit:** Flexibility without sacrificing governance.

---

## 3. Separate Bronze, Silver and Gold responsibilities

**Decision:** Do not combine ingestion, conformance and
analytical modelling.

**Reason:** Each layer has a different contract.

**Benefit:** Traceability, replayability and failure isolation.

---

## 4. Preserve source truth

**Decision:** Preserve original audit `run_status` and derive
operational classifications separately.

**Reason:** Monitoring interpretation should not rewrite
historical evidence.

**Benefit:** Audit integrity.

---

## 5. Quarantine instead of fabrication

**Decision:** Invalid relationship records are rejected to
quarantine.

**Reason:** Fabricating parent identifiers would hide a
data-integrity problem.

**Benefit:** Transparent and recoverable failure handling.

---

## 6. Reconciliation in addition to data quality

**Decision:** Validate both correctness and completeness.

**Reason:** Valid records do not guarantee that all expected
records arrived.

**Benefit:** Stronger confidence in curated data.

---

## 7. Idempotent processing

**Decision:** Design processing so rerunning the same input does
not create unintended duplicates.

**Reason:** Production pipelines are retried.

**Benefit:** Safer recovery from failures and reruns.

---

## 8. Operational monitoring as data

**Decision:** Persist and model execution telemetry.

**Reason:** Platform behaviour should be measurable.

**Benefit:** Historical reliability analysis and operational
diagnostics.

---

# Technology Stack

| Area | Technology |

|---|---|

| Cloud Data Platform | Microsoft Fabric |

| Orchestration | Fabric Data Factory |

| Distributed Processing | Apache Spark / PySpark |

| Storage Architecture | OneLake / Delta Lake |

| Data Engineering | Python, SQL |

| Operational API | FastAPI |

| Operational Database | PostgreSQL |

| Warehouse Source | SQL Server |

| File/Object Sources | SharePoint, AWS S3, Azure Blob Storage |

| Analytical Modelling | Fabric Lakehouse, dimensional modelling |

| Semantic Layer | Power BI semantic model |

| Reporting | Power BI |

| Version Control | Git / GitHub |

### Engineering patterns demonstrated

- metadata-driven orchestration

- incremental loading

- watermarking

- idempotent processing

- schema enforcement

- Delta MERGE

- record hashing

- data quality

- reconciliation

- quarantine handling

- audit logging

- operational monitoring

- dimensional modelling

- semantic modelling

- feature-branch development

- Pull Requests

---

# Repository Structure

```text

britmart-retail-data-platform/

│

├── README.md

│

├── docs/

│   ├── architecture/

│   └── evidence/

│

├── fabric/

│   ├── control-framework/

│   ├── pipelines/

│   └── notebooks/

│       ├── silver/

│       ├── gold/

│       └── monitoring/

│

└── sql/

    └── control-framework/

```

The Supplier API is maintained separately because it represents an
upstream operational source system rather than part of the Fabric
analytical platform.

**Supplier API repository:**  

[BritMart Supplier
API](https://github.com/Mayorpluz1/britmart-supplier-api)

---

# Security

No credentials, passwords, API keys, access tokens or connection strings
should be committed to this repository.

Sensitive configuration is maintained outside source control.

Screenshots and source files should be reviewed before publication to
ensure that authentication material and environment secrets are not
exposed.

---

# Portfolio Scope

BritMart is a **fictional UK retail organisation** created
specifically for this portfolio/reference implementation.

The architecture, generated datasets, source-system simulation,
pipelines, transformations, control framework, validation logic,
monitoring framework, semantic model and reporting layer were developed
to simulate realistic enterprise data-engineering requirements.

The project demonstrates practical capability in designing and
implementing an end-to-end analytical data platform. It does
**not** represent a production system operated by a real
BritMart organisation or client.