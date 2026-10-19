# End-to-End Data Engineering Project Using Databricks — FMCG Domain

## 1. Project Overview

This project demonstrates an end-to-end data engineering pipeline built using **Databricks Free Edition, Apache Spark, Python, SQL, and Amazon S3**.

The project addresses a real-world business scenario in the Fast-Moving Consumer Goods (FMCG) industry. A large retail company acquires a smaller company, and both organizations need to consolidate their data to support unified reporting and business decision-making.

The acquired company has data spread across different systems and files, while the parent company already has an established data processing architecture. The challenge is to integrate the acquired company's data into a consistent, reliable, analytics-ready data platform without disrupting the existing system.

The solution uses the **Medallion Architecture (Bronze → Silver → Gold)** to ingest, clean, standardize, transform, and consolidate data. Historical data is processed through a full load, while subsequent data is processed incrementally. The final datasets support dashboards and AI-assisted analytics.

**Project domain:** FMCG / Retail Analytics  
**Project type:** End-to-end Data Engineering / ETL Pipeline  
**Platform:** Databricks Free Edition  
**Reference tutorial:** [End to End Data Engineering Project using Databricks Free Edition | FMCG Domain](https://www.youtube.com/watch?v=U6ZUKWdfSLY)

---

## 2. Business Problem

### Business scenario

Consider two companies:

- **Atlon:** An established parent company with an existing data platform and analytics infrastructure.
- **Sports Bar:** A smaller acquired company with less mature data systems and data distributed across different sources.

After the acquisition, management wants to analyze both companies' sales, products, customers, and business performance in one place.

However, their data may have different formats, naming conventions, data quality standards, and reporting requirements.

### Challenges

1. Data from the two companies is stored in separate systems.
2. Customer, product, and sales attributes may follow different naming conventions.
3. Source data may contain null values, duplicates, inconsistent text, or invalid dates.
4. Historical data must be loaded to establish a complete reporting baseline.
5. New data must be processed regularly without unnecessarily reprocessing all historical records.
6. Consolidated data must follow a consistent structure so that dashboards produce meaningful results.
7. The pipeline must be repeatable and manageable as new data arrives.

### Project objective

Build a unified data engineering pipeline that:

- Ingests source data from Amazon S3.
- Preserves raw data for traceability.
- Cleans and standardizes data using Spark.
- Creates analytics-ready dimension and fact tables.
- Supports historical and incremental data processing.
- Consolidates the acquired company's curated data with the parent company's existing data.
- Automates processing through Databricks Jobs.
- Enables reporting through dashboards and natural-language analytics using Genie.

---

## 3. Technologies Used

| Technology | Purpose |
|---|---|
| Databricks Free Edition | Development environment for notebooks, Spark processing, SQL, tables, jobs, dashboards, and Genie capabilities |
| Python | Data-processing logic, reusable functions, and pipeline implementation |
| PySpark / Apache Spark | Distributed data transformation, cleansing, joins, aggregations, and large-scale processing |
| Spark SQL | Querying, validating, joining, and analyzing structured datasets |
| Amazon S3 | Object storage for source files and incoming data |
| Delta Lake | Transactional table storage for reliable data processing and merge operations, where implemented |
| Medallion Architecture | Separates raw, cleaned, and business-ready data into Bronze, Silver, and Gold layers |
| Databricks Unity Catalog | Organizes and governs data objects through catalogs, schemas, tables, and permissions |
| Databricks Jobs | Schedules and orchestrates pipeline tasks and their dependencies |
| Databricks Dashboards | Presents business KPIs and analytical trends |
| Databricks Genie | Enables natural-language questions over configured analytical data |

*The exact features and permissions available can depend on the Databricks edition and workspace configuration.*

---

## 4. Architecture Overview

The pipeline follows a Medallion Architecture.

```text
                 SOURCE DATA
                      |
           +----------+----------+
           |                     |
      Parent Company       Acquired Company
      Existing Data        Source Files
                                 |
                                 v
                         Amazon S3
                                 |
                                 v
                       Databricks / Spark
                                 |
                                 v
                         BRONZE LAYER
                       Raw Ingested Data
                                 |
                                 v
                         SILVER LAYER
                    Cleaned and Standardized
                                 |
                                 v
                          GOLD LAYER
                   Business-Ready Dimensions
                       and Fact Tables
                                 |
                                 v
                    Historical / Incremental
                       Fact Processing
                                 |
                                 v
                  Consolidated Gold Dataset
                     Parent + Acquired Data
                                 |
                     +-----------+-----------+
                     |                       |
                     v                       v
               BI Dashboards           Genie Analytics
```

The parent company's existing curated data is aligned with the acquired company's Gold data before consolidation. The exact implementation of each connection and table should follow the notebooks in the repository.

### Layer responsibilities

**Bronze — Raw data**

- Ingest source files with minimal transformation.
- Preserve source information for auditing and troubleshooting.
- Maintain a reliable starting point for downstream processing.
- Retain ingestion metadata when implemented.

**Silver — Cleaned data**

- Standardize column names and data types.
- Handle missing or invalid values according to business rules.
- Remove or manage duplicate records.
- Normalize inconsistent text and date formats.
- Prepare consistent dimension and fact datasets.

**Gold — Business-ready data**

- Build curated dimension and fact tables.
- Align the acquired company's data model with the parent company's model.
- Apply business transformations and aggregations.
- Merge historical and incremental results into consolidated datasets.
- Serve data to dashboards and analytical queries.

---

## 5. Why This Project Was Built

This project demonstrates how data engineers solve an integration problem rather than simply moving files from one location to another.

### 5.1 Data integration

When two companies use different systems, their data cannot always be combined directly. The pipeline standardizes their structures and business attributes to make combined reporting possible.

### 5.2 Data quality

Raw operational data is not always suitable for analytics. Cleaning, validation, and standardization help prevent incorrect totals and misleading reports.

### 5.3 Scalable data processing

Spark allows transformations to run across distributed compute resources instead of relying exclusively on a single machine's processing capacity.

### 5.4 Incremental processing

After historical data has been loaded, processing only new or changed data can reduce unnecessary computation and shorten recurring pipeline runs.

### 5.5 Separation of responsibilities

Bronze, Silver, and Gold layers make it easier to isolate ingestion problems, data quality issues, and business transformation errors.

### 5.6 Analytics readiness

Business users should not need to understand every raw source file. Gold tables provide consistent data structures for dashboards and analytical queries.

---

## 6. Step-by-Step Implementation

### Step 1: Understand the business requirements

Start by understanding the acquisition scenario, existing data architecture, source systems, reporting requirements, and stakeholder expectations.

Identify:

- What data comes from each company.
- Which dimensions and fact tables are required.
- Which attributes must be standardized.
- How historical data will be loaded.
- How new data will be processed.
- Which business metrics need to be reported.

**Why this matters:** A data pipeline should be designed around business requirements and data contracts, not just around the available tools.

### Step 2: Design the data architecture and model

Identify the source-to-target mappings and determine how the parent and acquired companies' datasets will align.

The project uses dimension and fact processing.

**Dimension tables** describe business entities such as customers, products, dates, or pricing.

**Fact tables** contain measurable business events, such as sales or orders, at a defined level of detail.

A typical analytical model may include:

- `dim_customers`
- `dim_products`
- `dim_date`
- `dim_gross_price`
- `fact_orders`

Use the actual table names defined in the implementation when documenting your completed repository.

**Why this matters:** A consistent data model makes cross-company reporting and analytical joins more reliable.

### Step 3: Set up Databricks

Create a Databricks Free Edition workspace and configure the necessary data objects.

Typical setup tasks include:

1. Create or select a catalog.
2. Create schemas for the required layers or data domains.
3. Prepare the necessary tables and notebooks.
4. Configure workspace permissions.
5. Verify that Spark and SQL execution work correctly.

Unity Catalog provides a logical organization and governance layer for data objects.

**Why this matters:** Organized catalogs and schemas help separate raw, intermediate, and curated datasets and make access management easier.

### Step 4: Configure Amazon S3

Prepare an S3 bucket and upload the source files supplied for the project.

Organize files into sensible locations, for example:

```text
s3://your-bucket/
├── source-data/
│   ├── dimensions/
│   ├── historical-facts/
│   └── incremental-facts/
└── archive/
```

This is an illustrative layout; use the folder structure required by your actual pipeline.

Configure secure access between Databricks and S3 using a supported identity and credential mechanism. Avoid embedding AWS access keys or secrets directly in notebooks or committing them to GitHub.

**Why this matters:** S3 provides durable object storage, while Databricks provides the compute and processing environment.

### Step 5: Ingest data into the Bronze layer

Read the source files into Spark DataFrames and persist the ingested records in the Bronze layer.

Typical operations include:

1. Read the source file.
2. Configure the correct format, header, and schema.
3. Preserve source values as much as practical.
4. Add ingestion metadata if required.
5. Write the raw records to the Bronze table.
6. Validate the ingested row count and schema.

**Why this matters:** Bronze separates data ingestion from downstream cleansing. If a transformation fails, engineers can investigate the original ingested data without relying on a previously cleaned version.

### Step 6: Process dimension data

Process dimension datasets before using them in fact transformations.

For each dimension:

1. Read the corresponding Bronze data.
2. Inspect the schema and profile the data.
3. Standardize column names and data types.
4. Trim unnecessary spaces and normalize inconsistent text.
5. Handle nulls and invalid values according to business rules.
6. Deduplicate records using the appropriate business key.
7. Generate surrogate keys where needed.
8. Validate the cleaned records.
9. Write the standardized data to Silver.
10. Apply the business model and write the required Gold dimension.

**Example:** If the same customer is represented with inconsistent capitalization or spacing, standardization can help align the values. However, matching records across different identifiers requires a defined business rule; text cleanup alone does not prove that two records represent the same customer.

**Why this matters:** Fact records need consistent dimension references to support accurate joins and reporting.

### Step 7: Process historical fact data

Historical fact processing establishes the initial analytical baseline.

The pipeline typically:

1. Reads the historical fact source.
2. Loads and validates the records.
3. Applies required data cleaning and type conversions.
4. Joins fact records with the relevant dimensions.
5. Applies business transformations and aggregations.
6. Writes the transformed records to staging or intermediate tables.
7. Merges or loads the results into the appropriate Gold fact table.
8. Validates counts and business totals.

For this scenario, historical Sports Bar data is processed before recurring incremental data is introduced.

**Why this matters:** A historical load provides the baseline against which future sales and business performance can be analyzed.

### Step 8: Implement incremental fact processing

After the initial historical load, new data arrives periodically. Reprocessing every historical file for each run can be inefficient.

The incremental workflow typically:

1. Identifies the new input files or records.
2. Reads the incremental dataset.
3. Validates the input schema and required fields.
4. Transforms the new records using the same business rules as the historical pipeline.
5. Joins the records with the required dimensions.
6. Aggregates or stages the transformed data.
7. Merges the results into the target Gold table.
8. Records or tracks successful processing.
9. Archives or otherwise manages processed source files according to the workflow.

The precise incremental detection and merge conditions must match the implementation. Processing a file twice should not unintentionally double-count its data.

**Example:** Suppose the initial load contains sales through November 30, and the next daily file contains sales for December 1. The pipeline should process the new records and update the target dataset without reloading all earlier files unnecessarily.

**Why this matters:** Incremental processing reduces redundant work and supports recurring data delivery.

### Step 9: Consolidate data from both companies

Once the acquired company's Gold tables are ready, align them with the parent company's existing curated data.

This requires consistent:

- Column names and data types.
- Business definitions.
- Dimension references and key strategies.
- Fact-table grain.
- Aggregation rules.
- Historical and incremental loading rules.

The final consolidation may use append, union, or merge operations depending on the table design and business requirements.

**Why this matters:** Combining tables without aligning their definitions can produce duplicate records, inconsistent KPIs, and incorrect totals.

### Step 10: Orchestrate the pipeline with Databricks Jobs

Once the individual notebooks and transformations work correctly, configure the required tasks and dependencies.

A conceptual workflow is:

```text
Ingest Source Data
        |
        v
Process Dimensions
        |
        v
Load Historical Facts
        |
        v
Process Incremental Facts
        |
        v
Consolidate Gold Data
        |
        v
Refresh Analytical Outputs
```

The actual dependencies should reflect which datasets each task consumes. For example, fact processing must not start before the required dimensions are available.

Configure schedules, task dependencies, failure notifications, and retry behavior where supported and appropriate.

**Why this matters:** Orchestration turns individually executable notebooks into a repeatable pipeline that can run without manual execution of every step.

### Step 11: Create a denormalized analytical dataset

Create a reporting-friendly table or view by joining the required fact and dimension tables.

A denormalized dataset can simplify common analytical queries by exposing useful business attributes alongside measures.

For example, it might contain:

- Order or sales date.
- Customer and market.
- Product and category.
- Sales channel.
- Quantity and revenue.

Use the actual columns and business definitions from your project.

**Why this matters:** BI tools and business users can query a consistent analytical dataset without repeatedly rebuilding complex joins.

### Step 12: Build dashboards and use Genie

Use the curated analytical data to create dashboards that help stakeholders explore business performance.

Possible analysis areas include:

- Sales trends over time.
- Revenue by product or category.
- Sales by customer, market, or channel.
- Period-over-period comparisons.
- Other KPIs defined by the business.

Configure Databricks Genie against appropriate analytical data so users can ask supported questions in natural language.

**Example questions:**

- Which product categories generate the highest revenue?
- How does monthly sales performance change over time?

Genie-generated answers should be validated against SQL queries and the underlying business definitions. Natural-language output does not automatically guarantee correct results.

**Why this matters:** The pipeline's value comes from the decisions it enables, not merely from the successful execution of Spark transformations.

### Step 13: Validate the end-to-end pipeline

Before considering the project complete, validate each layer and the final outputs.

Recommended checks include:

- Source and target row counts.
- Required columns and data types.
- Null and duplicate checks.
- Business-key uniqueness where expected.
- Fact-to-dimension join coverage.
- Revenue and quantity reconciliation.
- Historical load completeness.
- Incremental load correctness.
- Duplicate processing and rerun behavior.
- Dashboard results compared with independent SQL queries.

**Why this matters:** A pipeline that runs without errors can still produce incorrect business data. Data reconciliation is a separate requirement from successful execution.

---

## 7. Full Load vs. Incremental Load

| Aspect | Historical / Full Load | Incremental Load |
|---|---|---|
| Purpose | Establish the initial dataset or reload a defined historical scope | Process new or changed data |
| Input | Historical source records | Newly arrived or changed records |
| Processing scope | All records in the selected historical scope | Only the records identified for the incremental run |
| Typical use | Initial migration or backfill | Daily or recurring processing |
| Main risk | Long processing time and duplicate reloads | Missed records, repeated records, or incorrect merge conditions |
| Validation | Historical completeness and reconciliation | New-record counts, duplicate checks, and target reconciliation |

An incremental pipeline needs a reliable way to identify new or changed data. A date filter alone is not always sufficient, particularly when late-arriving records or corrections are possible.

---

## 8. Data Model: Facts and Dimensions

A dimensional model organizes analytical data into measurable events and descriptive entities.

### Fact table

A fact table represents business events at a defined grain.

For example, a sales fact table could represent sales for a product on a date through a particular channel, depending on the source data and model design.

Possible measures include:

- Quantity sold.
- Revenue.
- Other available sales measures.

### Dimension tables

Dimension tables provide context for the facts.

| Dimension | Example attributes |
|---|---|
| Customer | Customer code, customer name, market, platform, channel |
| Product | Product identifier, division, category, product name, variant |
| Date | Date, month, quarter, year |
| Gross Price | Product, year, month, gross price |

These are illustrative attributes based on the project's FMCG modeling scenario; use the actual source schema in the final implementation.

### Why dimensional modeling matters

Suppose a stakeholder asks for revenue by product category and month. A well-defined fact table connected to product and date dimensions makes the query straightforward and helps ensure that the same definitions are used across reports.

The fact table's grain, join keys, and aggregation rules must be explicitly defined to avoid double-counting.

---

## 9. Data Quality and Reliability

Data quality should be validated throughout the pipeline, not only after the Gold tables are created.

| Check | Purpose |
|---|---|
| Schema validation | Detect missing, renamed, or incompatible columns |
| Null validation | Identify missing values in required fields |
| Duplicate detection | Prevent repeated business records from distorting results |
| Data type validation | Ensure dates, numeric measures, and identifiers use appropriate types |
| Referential integrity | Identify fact records without valid dimension matches |
| Reconciliation | Compare source and target counts or business totals |
| Incremental validation | Confirm new records are processed without unintended duplication |
| Rerun validation | Check whether retrying a failed task changes the output incorrectly |

Invalid records should be rejected, quarantined, corrected, or accepted according to explicit business rules. Silently dropping records can hide important data-quality problems.

---

## 10. Repository Structure

An example structure for organizing the project is shown below. Adjust it to match your actual notebook names and files.

```text
databricks-fmcg-data-engineering/
│
├── README.md
│
├── notebooks/
│   ├── 01_environment_and_catalog_setup
│   ├── 02_data_ingestion
│   ├── 03_dimension_processing
│   ├── 04_historical_fact_processing
│   ├── 05_incremental_fact_processing
│   ├── 06_data_consolidation
│   ├── 07_analytical_dataset
│   └── 08_validation_and_quality_checks
│
├── architecture/
│   └── project_architecture.png
│
├── screenshots/
│   ├── bronze_layer.png
│   ├── silver_layer.png
│   ├── gold_layer.png
│   └── dashboard.png
│
└── .gitignore
```

This is a suggested layout, not a claim about the current contents of the repository.

Do not commit AWS credentials, access tokens, workspace secrets, or proprietary datasets. Confirm the dataset's redistribution terms before uploading any source files to a public repository.

---

## 11. How to Run the Project

### Prerequisites

- A Databricks Free Edition workspace.
- Basic knowledge of Python, Spark, and SQL.
- Access to the permitted project source datasets.
- An AWS account and an S3 bucket if reproducing the S3-based workflow.
- Appropriate, securely configured access to S3.
- Access to the Databricks features used by the notebooks.

### Execution steps

1. Review the business requirements and architecture.
2. Create the required catalog and schemas.
3. Configure source-data access.
4. Upload the permitted source files to S3.
5. Run the environment and ingestion notebooks.
6. Verify the Bronze tables.
7. Run dimension processing and verify Silver and Gold dimensions.
8. Run the historical fact load and reconcile the results.
9. Run the incremental fact pipeline and validate its behavior.
10. Consolidate the curated data.
11. Create or refresh the analytical dataset.
12. Configure the required Databricks Job tasks and dependencies.
13. Build the dashboard and configure Genie where available.
14. Run final data-quality and reconciliation checks.

**Important:** The exact execution order, notebook paths, table names, catalog configuration, and S3 access method must be taken from your implementation. This README provides the workflow, not executable notebook code.

---

## 12. Expected Outcomes

When the implementation is completed and validated, the project should demonstrate the ability to:

- Ingest source files from cloud object storage.
- Process data with Python and distributed Spark transformations.
- Separate raw, cleaned, and curated datasets.
- Build and maintain dimension and fact tables.
- Load historical data and process subsequent increments.
- Consolidate datasets from two business entities.
- Automate dependent processing tasks.
- Deliver curated data for dashboarding and analytical queries.
- Validate data quality and business totals throughout the pipeline.

These are project objectives and expected capabilities, not measured performance claims.

---

## 13. Challenges and Engineering Considerations

### Inconsistent source data

Different systems may represent the same business entity differently. Standardization and well-defined matching rules are necessary before merging records.

### Incremental data processing

A reliable pipeline must distinguish unprocessed records from records that have already been handled. Retries and late-arriving data need explicit treatment.

### Dimension and fact consistency

Fact records must reference the correct dimensions. Changes to business identifiers or dimension mappings can affect historical reporting.

### Schema changes

If a source adds, removes, or changes columns, downstream transformations may fail or silently produce incomplete outputs unless schema changes are detected and handled.

### Cost and permissions

Databricks and AWS access should follow least-privilege principles. Check workspace and cloud resource usage, storage retention, and any applicable usage limits.

### Observability

For a production implementation, add clear run status, record counts, error details, processing timestamps, and failure notifications so that pipeline issues can be diagnosed efficiently.

---

## 14. Possible Future Enhancements

The following are extensions to consider, not features to claim unless implemented:

- Add a metadata-driven ingestion framework.
- Implement Slowly Changing Dimensions (SCD Type 2) for historical dimension tracking where required.
- Add schema-drift detection and quarantine handling.
- Implement automated data-quality assertions.
- Add pipeline audit and reconciliation tables.
- Improve incremental processing with reliable checkpoints or watermarks.
- Add unit and integration tests.
- Improve monitoring and alerting.
- Optimize Spark joins, partitioning, and file sizes based on measured bottlenecks.
- Add CI/CD and automated deployment for notebooks and jobs.

---

## 15. Key Learning Outcomes

This project provides hands-on experience with:

- End-to-end data pipeline design.
- Databricks workspace and catalog setup.
- Amazon S3 integration.
- Python, PySpark, and Spark SQL.
- Medallion Architecture.
- Dimension and fact data processing.
- Historical and incremental ETL patterns.
- Data consolidation and reconciliation.
- Pipeline orchestration.
- Business analytics using dashboards and Genie.

The central lesson is that data engineering requires more than transforming data. A reliable solution must also preserve data lineage, enforce business rules, support repeatable processing, validate results, and deliver consistent datasets to downstream consumers.

---

## 16. Reference and Attribution

This project is based on a learning exercise inspired by the following tutorial:

**Video:** [End to End Data Engineering Project using Databricks Free Edition | FMCG Domain](https://www.youtube.com/watch?v=U6ZUKWdfSLY)

**Creator:** Codebasics

The README documents the business scenario and technical workflow for educational and portfolio purposes. Any code, datasets, images, or other resources used from the original tutorial should be attributed appropriately and used according to their applicable license or terms.

---

## Author

**Sai Charan**

Data Engineering | Databricks | PySpark | Python | SQL | AWS

GitHub: [SaiCharan211](https://github.com/SaiCharan211)
