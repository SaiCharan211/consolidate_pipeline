# FMCG Sports Bar — Consolidated Data Pipeline

## Overview

This project implements a **medallion architecture** (Bronze → Silver → Gold) data pipeline on **Databricks** for an FMCG (Fast-Moving Consumer Goods) company in the sports nutrition domain. Raw CSV files from Amazon S3 are ingested, cleaned, transformed, and enriched across multiple layers to produce analytics-ready dimension and fact tables in Unity Catalog. A **SCD Type 2** merge strategy preserves historical changes to customer attributes, and an **AI/BI Dashboard** provides sales insights to end users.

**Catalog:** `fmcg`

**Schemas:** `fmcg.bronze`, `fmcg.silver`, `fmcg.gold`

**Data Source:** `s3://sportsbar-pro/` (CSV files for customers, products, gross_price, and orders)

---

## Project Structure

```
consolidated_pipeline/
└── consolidate_pipeline/          ← Git root
    ├── 1_setup/
    │   ├── setup_catalogs            ← Creates the fmcg catalog and bronze/silver/gold schemas
    │   ├── dim_date_table_creation   ← Generates a month-grain date dimension table
    │   └── utilities                ← Shared notebook defining bronze_schema, silver_schema, gold_schema variables
    │
    ├── 2_dimension_date/
    │   ├── 1_customer_data_processing                    ← Customer dimension: Bronze → Silver → Gold (SCD2)
    │   ├── 1_customer_data_processing_backup_before_scd2 ← Pre-SCD2 backup of customer processing
    │   ├── 2_products_data_processing                    ← Product dimension: Bronze → Silver → Gold
    │   └── 3_pricing_data_processing                     ← Pricing dimension: Bronze → Silver → Gold
    │
    ├── 3_fact_data_processing/
    │   ├── 1_full_load_fact          ← Full-load orders fact: Bronze → Silver → Gold
    │   └── 2_incremental_load_fact   ← Incremental-load orders fact: Bronze → Silver → Gold
    │
    ├── New Notebook 2026-10-13 11_28_19  ← Exploratory SCD2 validation & data quality checks
    └── FMCG Dashboard.lvdash.json        ← AI/BI Dashboard with sales insights
```

---

## Pipeline Architecture

### Medallion Layers

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│     BRONZE        │ ──▶ │     SILVER        │ ──▶ │      GOLD         │
│  Raw ingestion    │     │  Cleaned &        │     │  Analytics-ready  │
│  (CSV → Delta)    │     │  transformed      │     │  dim & fact tables│
└──────────────────┘     └──────────────────┘     └──────────────────┘
```

| Layer | Purpose | Key Operations |
|-------|---------|----------------|
| **Bronze** | Raw data landing from S3 CSV files | Read CSV with schema inference, add `read_timestamp`, `_metadata.file_name`, `_metadata.file_size`, write as Delta with Change Data Feed (CDF) enabled |
| **Silver** | Data cleansing & enrichment | Deduplication, whitespace trimming, case normalization (`initcap`), city-name corrections, NULL handling, date format normalization, derived column creation |
| **Gold** | Production-ready tables | Staging tables (`sb_dim_*`, `sb_fact_*`) merged into parent dimension/fact tables with SCD2 (customers) or upsert (products, pricing, orders) logic |

---

## Setup Notebooks (`1_setup/`)

### 1. `setup_catalogs`

Creates the Unity Catalog infrastructure:

```sql
CREATE CATALOG IF NOT EXISTS fmcg;
USE CATALOG fmcg;
CREATE SCHEMA IF NOT EXISTS fmcg.gold;
CREATE SCHEMA IF NOT EXISTS fmcg.silver;
CREATE SCHEMA IF NOT EXISTS fmcg.bronze;
```

### 2. `dim_date_table_creation`

Generates a month-grain date dimension table (`fmcg.gold.dim_date`) covering January 2024 through December 2025.

**Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `month_start_date` | date | First day of each month |
| `date_key` | int | Surrogate key in `yyyyMM` format (e.g., 202401) |
| `year` | int | Year (e.g., 2024) |
| `month_name` | string | Full month name (e.g., "January") |
| `month_short_name` | string | Abbreviated month name (e.g., "Jan") |
| `quarter` | string | Quarter label (e.g., "Q1") |
| `year_quarter` | string | Combined year-quarter (e.g., "2024-Q1") |

### 3. `utilities`

A shared notebook imported via `%run` by all downstream notebooks. Defines three schema-name variables:

```python
bronze_schema = 'bronze'
silver_schema = 'silver'
gold_schema   = 'gold'
```

---

## Dimension Processing Notebooks (`2_dimension_date/`)

### 1. `1_customer_data_processing` — Customer Dimension

**Source:** `s3://sportsbar-pro/customers/*.csv`

#### Bronze Layer (`fmcg.bronze.customers`)
- Reads customer CSV files from S3 with header and schema inference
- Adds `read_timestamp`, `file_name`, and `file_size` metadata columns
- Writes as Delta table with Change Data Feed enabled
- **Raw data:** 39 rows (includes duplicates)

#### Silver Layer (`fmcg.silver.customers`)
- **Deduplication:** Removes 4 duplicate rows based on `customer_id` (39 → 35 rows)
- **Whitespace trimming:** Trims leading/trailing spaces from `customer_name`
- **City name corrections:** Fixes misspellings (e.g., "Bengalore" → "Bengaluru", "Hyderbad" → "Hyderabad", "NewDelhi" → "New Delhi")
- **NULL city handling:** Business-confirmed city assignments for customers with NULL city values (Sprintx Nutrition → New Delhi, Zenathlete Foods → Bengaluru, Primefuel Nutrition → Hyderabad, Recovery Lane → Hyderabad)
- **Name standardization:** Applies `initcap()` to `customer_name`
- **Derived columns:**
  - `customer` = `concat_ws('-', customer_name, city)`
  - `market` = 'India'
  - `platform` = 'Sports Bar'
  - `channel` = 'Acquisition'
- Writes as Delta with CDF enabled

#### Gold Layer

**Staging table:** `fmcg.gold.sb_dim_customers`

| Column | Type |
|--------|------|
| `customer_id` | string |
| `customer_name` | string |
| `city` | string |
| `customer` | string |
| `market` | string |
| `platform` | string |
| `channel` | string |

**Production table:** `fmcg.gold.dim_customers` (SCD Type 2)

| Column | Type | Description |
|--------|------|-------------|
| `customer_code` | string | Mapped from `customer_id` |
| `customer` | string | Customer name-city combination |
| `market` | string | Geographic market |
| `platform` | string | Sales platform |
| `channel` | string | Acquisition channel |
| `effective_start_date` | timestamp | When this version became active |
| `effective_end_date` | timestamp | When this version was superseded (NULL if current) |
| `is_current` | boolean | Whether this is the active version |

**SCD Type 2 Merge Logic:**

1. Reads the staging table (`sb_dim_customers`) and validates for NULL and duplicate `customer_code` values
2. Compares current source attributes against existing current records in `dim_customers`
3. Identifies **changed customers** (attribute mismatch using `eqNullSafe` comparison)
4. Identifies **new customers** (no existing match in target)
5. Stages three action types:
   - **Expire** — Set `is_current = false` and `effective_end_date` for old versions of changed customers
   - **Insert (changed)** — Insert new version for changed customers
   - **Insert (new)** — Insert genuinely new customers
6. Executes a single Delta `MERGE` operation to apply all changes atomically

> **Note:** An earlier `whenMatchedUpdateAll()` approach was deprecated because it overwrote matching attributes instead of preserving SCD2 history. The current SCD2 merge replaces it.

### 2. `2_products_data_processing` — Product Dimension

**Source:** `s3://sportsbar-pro/products/*.csv`

#### Bronze Layer (`fmcg.bronze.products`)
- Reads product CSV files with columns: `product_name`, `product_id`, `category`
- Adds metadata columns and writes as Delta with CDF
- **Raw data:** 20 rows (includes duplicates)

#### Silver Layer (`fmcg.silver.products`)
- **Deduplication:** 20 → 18 rows based on `product_id`
- **Category standardization:** Applies `initcap()` to category names (e.g., "energy bars" → "Energy Bars")
- **Spelling fix:** Corrects "Portien" → "Protein" in both `product_name` and `category`
- **Derived columns:**
  - `division` — Mapped from category (e.g., "Energy Bars" → "Nutrition Bars", "Granola & Cereals" → "Breakfast Foods")
  - `variant` — Extracted from parenthetical content in `product_name` (e.g., "(60g)" from the product name)
  - `product_code` — SHA-256 hash of `product_name` for stable uniqueness
  - `product_id` — Validated as numeric; non-numeric values set to `999999`
  - `product` — Renamed from `product_name`

#### Gold Layer

**Staging table:** `fmcg.gold.sb_dim_products` → Columns: `product_code`, `product_id`, `division`, `category`, `product`, `variant`

**Production table:** `fmcg.gold.dim_products` — Merged via Delta `MERGE` on `product_code` with `whenMatchedUpdate` / `whenNotMatchedInsert`

### 3. `3_pricing_data_processing` — Pricing Dimension

**Source:** `s3://sportsbar-pro/gross_price/*.csv`

#### Bronze Layer (`fmcg.bronze.gross_price`)
- Reads pricing CSV with columns: `product_id`, `month`, `gross_price`
- Raw `month` field has inconsistent formats (e.g., `2025/07/01`, `01/08/2025`, `2025-10-01`, `01-12-2025`)
- Raw `gross_price` has invalid values (e.g., negative numbers, "unknown" strings)

#### Silver Layer (`fmcg.silver.gross_price`)
- **Date normalization:** Uses `try_to_date` with multiple format patterns to standardize all `month` values to `yyyy-MM-dd`
- **Price cleansing:**
  - Converts numeric strings to `double`
  - Negates negative prices (absolute value)
  - Non-numeric values (e.g., "unknown") set to `0`
- **Product enrichment:** Inner joins with `fmcg.silver.products` to attach `product_code`

#### Gold Layer

**Staging table:** `fmcg.gold.sb_dim_gross_price` → Columns: `product_code`, `month`, `gross_price`

**Production table:** `fmcg.gold.dim_gross_price` — Merged via Delta `MERGE` on `product_code`

**Price selection logic:** For each `product_code` and `year`, selects the latest non-zero price using a window function (`row_number` partitioned by product_code + year, ordered by zero-price flag then month descending).

| Column | Type |
|--------|------|
| `product_code` | string |
| `price_inr` | double |
| `year` | string |

---

## Fact Processing Notebooks (`3_fact_data_processing/`)

### 1. `1_full_load_fact` — Full Load Orders

**Source:** `s3://sportsbar-pro/orders/landing/*.csv` (falls back to `s3://sportsbar-pro/orders/processsed/` if landing is empty)

#### Bronze Layer (`fmcg.bronze.orders`)
- Reads order CSV files with columns: `order_id`, `order_placement_date`, `customer_id`, `product_id`, `order_qty`
- Adds metadata columns and writes as Delta with CDF (append mode)
- Moves processed files from `landing/` to `processsed/` directory

#### Silver Layer (`fmcg.silver.orders`)
- **Data cleansing:**
  - Filters rows where `order_qty` is not NULL
  - Cleans `customer_id`: numeric values kept, non-numeric set to `999999`
  - Standardizes `order_placement_date` to `yyyy-MM-dd` format
- **Product enrichment:** Inner joins with `fmcg.silver.products` to attach `product_code`
- **Merge strategy:** If table doesn't exist, creates it (overwrite). If it exists, performs Delta `MERGE` on composite key (`order_placement_date`, `order_id`, `product_code`, `customer_id`)

#### Gold Layer

**Staging table:** `fmcg.gold.sb_fact_orders` — Daily-grain fact table

| Column | Type |
|--------|------|
| `order_id` | string |
| `date` | date |
| `customer_code` | string |
| `product_code` | string |
| `product_id` | string |
| `sold_quantity` | double |

**Production table:** `fmcg.gold.fact_orders` — Monthly-grain fact table

- Aggregates daily data to monthly grain: `trunc(date, 'MM')` grouped by `date`, `product_code`, `customer_code` with `SUM(sold_quantity)`
- Merged via Delta `MERGE` on composite key (`date`, `product_code`, `customer_code`)

| Column | Type |
|--------|------|
| `date` | date |
| `product_code` | string |
| `customer_code` | string |
| `sold_quantity` | double |

### 2. `2_incremental_load_fact` — Incremental Load Orders

Handles incremental/periodic loads of new order files arriving in the landing zone.

#### Key Differences from Full Load:
- **New file detection:** Checks `landing/` folder for new CSV files; exits early (`dbutils.notebook.exit`) if none found
- **Staging tables:** Uses temporary `fmcg.bronze.staging_orders` and `fmcg.silver.staging_orders` tables to isolate incremental data
- **Incremental monthly recalculation:**
  1. Extracts distinct month-start dates from the staging table
  2. Pulls all daily rows from `sb_fact_orders` matching those months
  3. Re-aggregates to monthly grain (re-sums `sold_quantity` per month/product/customer)
  4. Merges recalculated monthly data into `fact_orders` (upsert)
- **Cleanup:** Drops staging tables after processing

---

## Exploratory Notebook (`New Notebook 2026-10-13 11_28_19`)

A SQL-based exploratory notebook for validating the SCD2 customer dimension:
- Lists all tables in `fmcg.gold`
- Tests table creation/drop for SCD2 scenarios
- Describes and samples `dim_customers` and `sb_dim_customers`
- Runs data quality checks:
  - Counts current vs. historical rows in `dim_customers`
  - Validates `effective_start_date` / `effective_end_date` / `is_current` consistency
  - Compares source vs. target customer counts (source-only, target-only, both)
  - Identifies customers in target not present in source (anti-join)
  - Classifies source customers as NEW, CHANGED, or UNCHANGED via attribute comparison

---

## FMCG Dashboard

An **AI/BI Lakeview dashboard** built on top of the gold-layer view `fmcg.gold.vw_fact_orders_enriched`.

### Dashboard Pages

#### Page 1: Sales Insights

| Widget | Type | Description |
|--------|------|-------------|
| Sales Insights | Text box | Dashboard title |
| Total Revenue | Counter | Sum of `total_amount_inr` |
| Total Quantity | Counter | Sum of `sold_quantity` |
| Year filter | Filter (single-select) | Filter by year |
| Quarter filter | Filter (single-select) | Filter by quarter |
| Channel filter | Filter (single-select) | Filter by channel |
| Month filter | Filter (single-select) | Filter by month |
| Revenue by Channel | Pie chart | Revenue distribution across channels |
| Top 10 Products by Revenue | Bar chart | Highest-revenue products |
| Monthly Revenue Generated | Line chart | Revenue trend across months |
| Product Price vs Quantity | Scatter chart | Price vs. quantity relationship, colored by category |
| Quantity Sold by Year, Month & Category | Bar chart | Multi-dimensional quantity breakdown |
| Top 10 Customers | (Details) | Top customers by revenue and quantity |

#### Page 2: Global Filters
- Global cross-dashboard filter controls

### Dashboard Datasets

| Dataset | Query |
|---------|-------|
| `vw_fact_orders_enriched` | Main dataset from the enriched gold view |
| Top 10 Products | `SELECT product, SUM(total_amount_inr) FROM vw_fact_orders_enriched GROUP BY product ORDER BY total_revenue DESC LIMIT 10` |
| Top 10 Customers | `SELECT customer, SUM(total_amount_inr) FROM vw_fact_orders_enriched GROUP BY customer ORDER BY total_revenue DESC LIMIT 10` |
| Top 10 Customers Details | `SELECT customer, SUM(sold_quantity), SUM(total_amount_inr) FROM vw_fact_orders_enriched GROUP BY customer ORDER BY total_revenue DESC LIMIT 10` |

---

## Complete Table Inventory

### Bronze Layer

| Table | Source | Description |
|-------|--------|-------------|
| `fmcg.bronze.customers` | `s3://sportsbar-pro/customers/*.csv` | Raw customer data |
| `fmcg.bronze.products` | `s3://sportsbar-pro/products/*.csv` | Raw product data |
| `fmcg.bronze.gross_price` | `s3://sportsbar-pro/gross_price/*.csv` | Raw pricing data |
| `fmcg.bronze.orders` | `s3://sportsbar-pro/orders/landing/*.csv` | Raw order data |

### Silver Layer

| Table | Description |
|-------|-------------|
| `fmcg.silver.customers` | Deduplicated & enriched customer data |
| `fmcg.silver.products` | Cleaned & enriched product data |
| `fmcg.silver.gross_price` | Normalized pricing data with product_code |
| `fmcg.silver.orders` | Cleansed orders with product_code enrichment |

### Gold Layer — Staging Tables

| Table | Description |
|-------|-------------|
| `fmcg.gold.sb_dim_customers` | Clean staging for customer dimension |
| `fmcg.gold.sb_dim_products` | Clean staging for product dimension |
| `fmcg.gold.sb_dim_gross_price` | Clean staging for pricing dimension |
| `fmcg.gold.sb_fact_orders` | Daily-grain staging fact table |

### Gold Layer — Production Tables

| Table | Type | Description |
|-------|------|-------------|
| `fmcg.gold.dim_customers` | SCD Type 2 | Customer dimension with historical versions |
| `fmcg.gold.dim_products` | Upsert | Product dimension |
| `fmcg.gold.dim_gross_price` | Upsert | Pricing dimension (latest price per product per year) |
| `fmcg.gold.dim_date` | Static | Month-grain date dimension (2024–2025) |
| `fmcg.gold.fact_orders` | Upsert | Monthly-grain orders fact table |
| `fmcg.gold.vw_fact_orders_enriched` | View | Enriched fact view joining fact + dimensions + pricing |

### Backup Tables

| Table | Description |
|-------|-------------|
| `fmcg.gold.dim_customers_backup` | Deep clone of `dim_customers` (pre-SCD2 backup) |

---

## Data Flow Diagram

```
                    S3 (s3://sportsbar-pro/)
                    ├── customers/*.csv
                    ├── products/*.csv
                    ├── gross_price/*.csv
                    └── orders/landing/*.csv
                         │
    ┌────────────────────┼────────────────────┐
    │         BRONZE LAYER (Raw Delta + CDF)    │
    │   bronze.customers  bronze.products       │
    │   bronze.gross_price  bronze.orders       │
    └────────────────────┬─────────────────────┘
                         │
    ┌────────────────────┼────────────────────┐
    │         SILVER LAYER (Cleansed + Enriched)│
    │   silver.customers  silver.products      │
    │   silver.gross_price  silver.orders       │
    └────────────────────┬─────────────────────┘
                         │
    ┌────────────────────┼────────────────────┐
    │      GOLD STAGING (sb_dim_* / sb_fact_*)  │
    │   sb_dim_customers  sb_dim_products       │
    │   sb_dim_gross_price  sb_fact_orders       │
    └────────────────────┬─────────────────────┘
                         │
    ┌────────────────────┼────────────────────┐
    │     GOLD PRODUCTION (dim_* / fact_*)      │
    │   dim_customers (SCD2)  dim_products      │
    │   dim_gross_price  dim_date  fact_orders  │
    │   vw_fact_orders_enriched (view)          │
    └────────────────────┬─────────────────────┘
                         │
                ┌────────┴────────┐
                │  FMCG Dashboard   │
                │  (AI/BI Lakeview) │
                └─────────────────┘
```

---

## Key Design Decisions

1. **Change Data Feed (CDF):** Enabled on all Bronze tables to support incremental processing and auditability
2. **SCD Type 2 for Customers:** Preserves full history of customer attribute changes with `effective_start_date`, `effective_end_date`, and `is_current` columns
3. **SHA-256 Product Codes:** Products are keyed by a stable hash of the product name, ensuring consistent joins across tables
4. **Monthly Aggregation:** Fact orders are aggregated from daily to monthly grain before loading into the parent fact table
5. **Incremental Recalculation:** The incremental load identifies affected months and re-aggregates only those partitions, avoiding full table scans
6. **File Lifecycle Management:** Order files are moved from `landing/` to `processsed/` after ingestion to prevent reprocessing
7. **Shared Utilities:** All notebooks import a common `utilities` notebook via `%run` for consistent schema naming
8. **Widget Parameters:** Each notebook uses `dbutils.widgets` for parameterized catalog and data_source values, enabling reuse across environments

---

## Execution Order

For a fresh setup, run notebooks in this order:

1. `1_setup/setup_catalogs` — Create catalog and schemas
2. `1_setup/dim_date_table_creation` — Create date dimension
3. `2_dimension_date/1_customer_data_processing` — Customer dim (Bronze → Gold)
4. `2_dimension_date/2_products_data_processing` — Product dim (Bronze → Gold)
5. `2_dimension_date/3_pricing_data_processing` — Pricing dim (Bronze → Gold)
6. `3_fact_data_processing/1_full_load_fact` — Initial full load of orders fact
7. `3_fact_data_processing/2_incremental_load_fact` — Schedule for ongoing incremental loads

---

## Technologies Used

| Technology | Role |
|-----------|------|
| Databricks | Compute & notebook execution environment |
| Apache Spark (PySpark) | Data processing framework |
| Delta Lake | Storage format with ACID transactions, CDF, and MERGE support |
| Unity Catalog | Data governance and access control (`fmcg` catalog) |
| Amazon S3 | Cloud storage for raw CSV source files |
| AI/BI Lakeview | Dashboard visualization |
| SQL | Exploratory analysis and validation queries |

---

## Serverless Compute Notes

The workspace uses **serverless (Spark Connect)** compute. Key constraints observed:
- `DeltaTable` Python API (`DeltaTable.forName`, `.merge()`) may not be fully supported — use SQL `MERGE INTO` via `spark.sql()` as fallback
- `.cache()` / `.persist()` are not supported (use `F.lit()` with a Python-captured timestamp instead of `F.current_timestamp().cache()`)
- Python cells in a SQL-default notebook need `%python` magic at the top

---

## License

This project is proprietary. All rights reserved.
