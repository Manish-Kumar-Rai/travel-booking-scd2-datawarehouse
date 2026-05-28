
# Travel Booking SCD2 Data Warehouse

A production-grade, end-to-end data engineering pipeline designed to ingest raw transactional travel data, enforce data quality gates, maintain historical dimensions using Slowly Changing Dimensions Type 2 (SCD2), and expose an optimized analytical layer for business intelligence.

Built entirely on the **Databricks Lakehouse Platform**, the project leverages **Delta Lake**, **Unity Catalog**, **PyDeequ**, and **Databricks Workflows** to deliver a highly reliable, idempotent, and auditable data platform.

---

## 🏗️ Project Architecture & Data Flow

The project follows the Medallion Architecture pattern, orchestrated across multi-layered schemas managed via Unity Catalog:

```
[Databricks Volumes] (Raw JSON/CSV)

                       │

                       ▼ (Idempotent Ingestion)

      ┌─────────────────────────────────┐

      │       bronze.booking_inc        │

      │       bronze.customer_inc       │

      └─────────────────────────────────┘

                       │

                       ▼ (Data Quality Gates via PyDeequ) ──> [ops.dq_results / ops.dq_daily_summary]

      ┌─────────────────────────────────┐

      │   default.customer_dim (SCD2)   │

      │      default.booking_fact       │

      └─────────────────────────────────┘

                       │

                       ▼ (Analytical Aggregations)

      ┌─────────────────────────────────┐

      │  analytics.daily_revenue_type   │

      │   analytics.customer_360_view   │

      └─────────────────────────────────┘
```

1\. **Ingestion (Bronze)**: Incremental batch loads pull data from configured **Databricks Volumes** into Bronze tables (`booking_inc`, `customer_inc`) using idempotent appends and auditing metadata.
2\. **Data Quality Gate (PyDeequ)**: Robust statistical data profiling checks for schema compliance, completeness, and non-negativity before downstream processing.
3\. **Core Warehouse (Silver/Default)**:
   * **Customer Dimension**: Tracks historical attribute changes using an automated SCD2 `MERGE` pattern (effective/expiry tracking).
   * **Booking Fact**: Enriches records with dimension surrogate keys (`customer_sk`) at a daily grain using safe upserts.
4\. **Analytics (Gold)**: Aggregated business layers optimized for end-user reporting.

---

## 🛠️ Tech Stack & Ecosystem

* **Compute & Execution**: Databricks Runtime (Apache Spark 3.5 / PySpark)
* **Storage Layer**: Delta Lake with ACID transactions, Time Travel, and unified metadata
* **Data Governance**: Unity Catalog (`travel_dwh` catalog) for fine-grained lineage and access control
* **Data Quality Framework**: `PyDeequ` (Deequ library port for Spark) for declarative data quality assertions
* **Orchestration**: Databricks Workflows (Notebook dependency graphs + parameterized SQL tasks)

---

## 📁 Repository Structure

```text
travel-booking-scd2-datawarehouse/
├── Notebooks/
│   ├── 01_bronze_ingestion.py      # Parameterized volume-to-table ingestion
│   ├── 02_data_quality_pydeequ.py  # PyDeequ validation suite & logging
│   ├── 03_silver_customer_scd2.py  # SCD2 tracking merge script
│   └── 04_silver_booking_fact.py   # Fact table surrogate key map & merge
├── sql_queries/
│   ├── daily_revenue_summary.sql   # Idempotent delete-and-insert revenue marts
│   └── customer_360_metrics.sql    # Comprehensive consumer analytics view
├── .gitignore
├── projectStructure.txt
└── README.md

```

🚀 Pipeline Implementations
---------------------------

### 1\. Incremental Ingestion & Idempotency

All ingestion pipelines are parameterized by `arrival_date`. The bronze layer utilizes precise filter-and-append strategies ensuring that reprocessing the same day's file generates identical warehouse states without duplicate entries.

### 2\. Enterprise Data Quality Framework (PyDeequ)

Using a cluster attached with Maven coordinates for `com.amazon.deequ:deequ:2.0.7-spark-3.5`and `pydeequ`via PyPI, the system executes unit tests on data including:

-   **Completeness** : Constraints verifying critical keys (like `customer_id`) are never null.

-   **Integrity** : Non-negativity constraints on price, quantities, and valid categorization constraints on `booking_type`.

-   **Auditability** : Execution statistics are parsed and logged straight into operational metadata stores ( `ops.dq_results`and `ops.dq_daily_summary`).

### 3\. Slowly Changing Dimensions (SCD2)

The `default.customer_dim`table maintains full historical fidelity. When a customer profile changes:

-   **Close Active Record** : The existing record's `valid_to`timestamp is updated from `9999-12-31`to the current operational business date, and `is_current`transitions to `false`.

-   **Insert New Record** : The updated profile is appended with a new, cryptographically secure or sequenced surrogate key ( `customer_sk`), `valid_from`set to the business date, and `is_current`flagged as `true`.

### 4\. Performance & Operations Optimization

To combat file fragmentation (the "small file problem") common with continuous batch updates, the pipeline automatically executes physical optimizations:

SQL

```
OPTIMIZE default.booking_fact ZORDER BY (business_date, customer_sk);
VACUUM default.booking_fact RETAIN 168 HOURS;
ANALYZE TABLE default.booking_fact COMPUTE STATISTICS FOR ALL COLUMNS;

```

📊 Operational & Metadata Schemas
---------------------------------

The platform self-monitors through an isolated operational schema ( `ops`), capturing telemetry across every pipeline invocation.

### `ops.run_log`

Tracks global pipeline state executions and runtime params.

| **run_id** | **arrival_date** | **stage** | **status** | **message** | **recorded_at** |
| --- | --- | --- | --- | --- | --- |
| `nb-val-2025-09-24...` | 2025-09-24 | `validate_inputs` | STARTED | Inputs validated | 2026-05-28T12:38:49 |

### `ops.dq_daily_summary`

Aggregates health profiles across target physical assets.

| **business_date** | **dataset** | **checks_passed** | **checks_failed** | **recorded_at** |
| --- | --- | --- | --- | --- |
| 2025-09-24 | `booking_inc` | 6 | 0 | 2026-05-28T12:41:40 |
| 2025-09-24 | `customer_inc` | 4 | 0 | 2026-05-28T12:41:40 |

🔄 Workflow Orchestration
-------------------------

The platform relies on multi-task** Databricks Workflows **allowing for concurrent analytical computations while enforcing strict sequentially where required:

-   **Task 1:`travel_booking_init`** - Sets workspace parameters, verifies input volumes, initializes operational tables.

-   **Task 2: Downstream Parallel Processing**

    -   `customer_360_sql`: Evaluates consumer dynamic shifts and dimensions profiles.

    -   `daily_revenue_summary`: Generates transactional financial reports for down-stream tools.

    -   `data_quality_summary`: Triggers PyDeequ checks on delta entities, raising immediate visual warnings/alerts if thresholds break.

-   **Task 3:`log_completion_flow`** --- Collects metadata and records a uniform success log ( `ops.workflow2_run_log`).

📈 Sample Analytical Output ( `analytics.daily_revenue_by_type`)
----------------------------------------------------------------

| **business_date** | **booking_type** | **total_amount** | **total_quantity** |
| --- | --- | --- | --- |
| **2025-09-24** | Flight | 44,911.0 | 298 |
| **2025-09-24** | Hotel | 42,775.0 | 294 |
| **2025-09-23** | Hotel | 23,198.0 | 171 |
| **2025-09-23** | Flight | 20,971.0 | 123 |
