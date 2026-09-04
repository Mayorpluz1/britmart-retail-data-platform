# Gold Analytics Layer

The Gold layer converts validated Silver datasets into business-ready dimensional models for analytics and Power BI.

The design uses fact and dimension tables with surrogate keys and explicit relationship validation.

## Gold Processing Flow

```text
Validated Silver Data
        │
        ▼
Conformed / Source-Aligned Dimensions
        │
        ▼
Surrogate Key Resolution
        │
        ▼
Fact Table Construction
        │
        ▼
Relationship Validation
        │
        ▼
Silver-to-Gold Reconciliation
        │
        ▼
Gold Delta Tables
        │
        ▼
Power BI Semantic Model
```

## Gold Notebooks

| Notebook | Responsibility |
|---|---|
| `40_nb_gold_dimensions` | Builds analytical dimensions |
| `41_nb_gold_fact_sales` | Builds unified sales fact |
| `42_nb_gold_fact_purchase_order` | Builds purchase-order and supplier-shipment facts |
| `44_nb_gold_fact_logistics_event` | Builds logistics event fact |
| `45_nb_gold_fact_supplier_performance` | Builds supplier-performance fact |
| `90_nb_gold_data_quality` | Validates Gold keys, relationships and reconciliation |

Operational pipeline monitoring is handled separately in `92_nb_gold_pipeline_monitoring`.

## Dimensions

The Gold model contains:

- `dim_date`
- `dim_supplier`
- `dim_distribution_centre`
- `dim_pos_store`
- `dim_ecommerce_store`
- `dim_pos_product`
- `dim_ecommerce_product`
- `dim_customer`
- `dim_sales_channel`

POS and e-commerce store and product dimensions remain separate because the source systems do not currently provide an authoritative enterprise crosswalk.

This avoids creating artificial mappings solely for reporting convenience.

## Fact Tables

The business Gold layer contains:

- `fact_sales`
- `fact_purchase_order`
- `fact_supplier_shipment`
- `fact_logistics_event`
- `fact_supplier_performance`

### Sales

`fact_sales` combines POS and e-commerce sales at line-level grain while retaining the originating sales channel.

### Procurement

`fact_purchase_order` provides purchase-order-level analytical measures and dimensional relationships.

### Supplier Shipments

`fact_supplier_shipment` represents supplier shipment activity and supports procurement and supplier delivery analysis.

### Logistics Events

`fact_logistics_event` represents operational logistics events.

Logistics-to-shipment relationships are validated as data-quality controls rather than implemented as a semantic fact-to-fact relationship.

### Supplier Performance

`fact_supplier_performance` provides event-level supplier performance information and validated references to relevant procurement entities.

## Gold Data Quality

The Gold validation framework checks:

- primary-key uniqueness
- null primary keys
- dimensional foreign-key resolution
- orphan relationships
- date-key integrity
- Silver-to-Gold reconciliation

Current validation evidence:

| Validation | Result |
|---|---:|
| Gold relationship checks | 23 / 23 passed |
| Silver-to-Gold reconciliation | 6 / 6 passed |

## Modelling Decisions

### Separate POS and E-commerce Master Data

POS and e-commerce store and product identifiers are not artificially merged.

Without an authoritative cross-system mapping, maintaining separate dimensions preserves source-system integrity and avoids unsupported assumptions.

### No Analytical Fact-to-Fact Relationships

Relationships such as logistics events to supplier shipments are useful for engineering validation but are not exposed as direct fact-to-fact relationships in the analytical model.

Dimensional relationships remain the primary semantic modelling pattern.

### Surrogate Keys

Dimensions use surrogate keys to decouple analytical relationships from operational source identifiers.

Business keys are retained for traceability.

## Power BI Integration

Gold tables provide the curated analytical layer consumed by the BritMart Power BI semantic model.

The semantic model supports reporting across:

- executive performance
- sales
- procurement
- supplier performance
- operational monitoring

The model is refreshed as the final stage of the end-to-end orchestration pipeline.
