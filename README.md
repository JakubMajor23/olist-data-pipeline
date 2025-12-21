<div align="center">

<p>
  <strong>🇺🇸 English</strong> | <a href="README.pl.md">🇵🇱 Polski</a>
</p>

# Olist E-commerce Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7%2B-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt--Core-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker--Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![SQLFluff](https://img.shields.io/badge/SQLFluff-Expected_Quality-00C7B7?style=for-the-badge&logo=sql&logoColor=white)
![CI Status](https://github.com/jakubmajor23/olist-data-pipeline/actions/workflows/ci_checks.yml/badge.svg)

<br>

**Complete, scalable ELT (Extract, Load, Transform) system simulating a production e-commerce environment.**

[Context](#-context--goals) •
[Tech Stack](#-tech-stack) •
[Architecture](#-architecture--data-flow) •
[Key Solutions](#-key-technical-solutions) •
[Setup](#-setup--usage)

</div>

---

## Context & Goals

**Problem:** Raw Olist data consists of scattered transaction logs. Analyzing revenue, delivery delays, or customer retention requires joining multiple tables, which is inefficient for real-time reporting.

**Solution:** An automated data pipeline that transforms raw logs into a clean **Galaxy Schema** (Fact Constellation) within the Data Warehouse.

### Project Documentation (Live)
The project includes fully generated dbt documentation available online:
**[Click here to view Data Lineage and Data Dictionary](https://jakubmajor23.github.io/olist-data-pipeline/)**

### Main Objectives
- **Single Source of Truth:** Centralized data for Orders, Payments, and Products.
- **Scalability:** Implemented **Incremental Loading** to handle growing data volumes efficiently.
- **Data Quality:** Enforced Referential Integrity, Idempotency, and automated `dbt` tests.

---

## Tech Stack

The project adheres to **Modern Data Stack** principles:

| Area | Implementation |
| :--- | :--- |
| **Orchestration** | **Event-Driven Airflow**: DAGs are triggered via REST API immediately after new data arrival (simulated). |
| **Modeling** | **Galaxy Schema**: Fact Constellation architecture (3 fact tables) eliminating Fan-out and Cartesian product issues. |
| **Transformation** | **dbt Core**: Models materialized as `incremental` and `table`, utilizing Jinja macros (DRY) and data tests. |
| **Code Quality** | **SQLFluff**: SQL Linter ensuring consistent code style and Postgres dialect compliance. |
| **Infrastructure** | **Docker & Docker Compose**: Full containerization of Airflow (with dbt) and Postgres database. |
| **Automation** | **GitHub Actions (CI/CD)**: Automated Linting on PRs and Documentation Deployment. |

---

## Architecture & Data Flow

The system is designed with modularity in mind, separating the simulation layer from the actual processing pipeline.

### End-to-End Data Lifecycle

The process simulates a real-world Data Warehouse operation in incremental mode:

1.  **Transaction Simulation (`simulate_production.py`):**
    Fetches historical data (month by month) and loads it into the operational database (`postgres-olist-source`), preserving strict Foreign Key constraints (Customers → Orders → Payments).

2.  **Trigger API (`run_demo.py`):**
    Immediately after loading, the orchestrator sends a POST request to the Airflow REST API, passing the `logical_date`. This ensures precise processing of only the new time slice.

3.  **Extract & Load (Airflow DAG):**
    * **Idempotency:** Before loading, the DAG cleans up the `raw_data` layer for the target month to prevent duplicates.
    * **Transfer:** Data is moved from Source DB to DWH (Raw Layer) using efficient SQLAlchemy engines.

4.  **Transformation (dbt):**
    Airflow triggers a dbt container (`dbt run`). Raw data is cleaned (Staging) and modeled into Facts and Dimensions (Marts).

5.  **Validation (`validate_data.py`):**
    A QA script verifies data integrity by comparing row counts between original CSV files and the **Source DB**, ensuring the simulation ran without data loss.

---

## Transformation Details (dbt)

The transformation layer is divided into two stages following Analytics Engineering best practices:

### Staging Layer (Raw -> Staging)
* Materialized as `incremental` for high-volume tables (Orders, Payments) and `table` for dictionaries.
* **Fail Fast** logic: `dbt_project.yml` enforces uniqueness tests on primary keys.

### Marts Layer (Staging -> Facts/Dims)
The **Galaxy Schema** model connects business processes via *Conformed Dimensions*.

| Fact Table | Logic & Purpose |
| :--- | :--- |
| **fact_orders** | Central transactional table. Aggregates cart values (`SUM(price)`), freight costs, and combines order statuses with reviews. |
| **fact_sales_items** | Granular table (Product-in-Cart level). Enables sales analysis by Product (`product_id`) and Seller (`seller_id`). |
| **fact_payments** | Analysis of cash flows, payment types (credit card, voucher), and installments (`payment_installments`). |
| **fact_reviews** | Dedicated fact table for handling review text and scores, resolving data loss issues (recovering 100k+ reviews vs 3k). |

---

## Key Technical Solutions

This project implements advanced Data Engineering patterns beyond standard ETL tutorials.

### 1. Performance & Modeling (dbt)
* **Fan-out Elimination (Galaxy Schema):** Deliberate decision to split data into 3 fact tables (`fact_orders`, `fact_sales_items`, `fact_payments`). This prevents Cartesian explosion (row duplication) and aggregation errors common in One-to-Many-to-Many relationships.
* **Surrogate Keys & Incremental Logic:** The `stg__order_items` table lacks a native primary key. This was solved by generating a deterministic surrogate key using `MD5(order_id || '-' || item_id)` to enable efficient incremental loading.
* **Ghost Records (Unknown Members):** Handling missing foreign keys (e.g., in `dim_products`). Invalid relationships are mapped to an artificial `MD5('unknown')` record to prevent data loss in `INNER JOIN` operations.

### 2. Architecture & Reliability
* **Transactional Idempotency (Savepoints):** Custom Airflow mechanism (`begin_nested()`) ensures data for the target period is cleared before reloading. This guarantees no duplicates and safe rollbacks.
* **Fail-Fast Strategy:** The pipeline intentionally breaks at startup if critical environment variables (passwords) are missing, preventing "silent failures" or insecure operations.
* **Integrity Simulation:** The data loader emulator respects Foreign Key constraints, inserting data in the correct order.

### 3. Data Quality & Cleaning
* **Data Repair (Imputation):** Staging logic imputes missing `order_approved_at` timestamps using the purchase date, flagging the record with `is_imputed`.
* **Geo Validation (Data Enrichment):** Instead of hard deletion, data is enriched with quality metadata. Coordinates falling outside Brazil's boundaries receive `is_valid_brazilian_location = False`.
* **Golden Record (Customers):** `dim_customers` uses window functions to deduplicate address changes, selecting the most recent location data.

### 4. Data Quality & Findings
* **Review Data Recovery:** Addressed a critical data loss issue where only ~3,110 reviews were initially captured. By decoupling reviews into `fact_reviews`, the system now correctly handles over 100k review records from the source CSV.
* **Geolocation Aggregation:** The raw dataset contains ~1 million geolocation records. These are aggressively aggregated to ~19k unique `zip_code_prefix` entries in `dim_geolocation` to serve as a lookup table, with 100% prefix coverage.
* **Customer Deduplication:** Removed ~3,345 duplicate customer records to ensure `customer_unique_id` integrity.
* **Staging Cleaning:** One product record is intentionally dropped during `stg_products` processing due to missing or invalid critical fields.

### 5. Configuration & Usability
* **Code as Configuration (Seeds):** Mapping dictionaries (e.g., order statuses) are managed as `dbt seeds` (CSV) rather than hardcoded `CASE WHEN` statements in SQL. This allows business analysts to update rules without modifying engineering code.
* **Translation Layer:** Automatic standardization of product categories (PT -> EN) via dictionary joins, adhering to DRY principles and simplifying global reporting.

### 6. Automation (CI/CD)
* **Automated Linting (CI):** Every Pull Request is verified by **GitHub Actions** running `sqlfluff`. This blocks non-compliant code from being merged.
* **Documentation Deployment (CD):** The documentation deployment process is fully automated. Upon merging to `main`, GitHub Actions spins up a **temporary database service**, compiles the dbt project, and publishes the updated site to GitHub Pages.

---

## Data Model

### Galaxy Schema
The project utilizes a **Fact Constellation** architecture where three fact tables share **Conformed Dimensions**.

<div align="center">
  <img src="readme_images/dwh.png" alt="Data Warehouse Architecture" width="100%">
</div>

| Fact Table | Description |
| :--- | :--- |
| **fact_orders** | Central transactional table. Aggregates cart values, freight costs, and combines statuses with reviews. |
| **fact_sales_items** | Granular table. Enables sales analysis by Product and Seller. |
| **fact_payments** | Analysis of cash flows, payment types, and installments. |
| **fact_reviews** | Dedicated fact table for reviews and scores. |

### Key Modeling Decisions
* **Role-Playing Dimensions:** `dim_date` connects to `fact_orders` multiple times. This allows simultaneous analysis of different lifecycle events (Purchase Date, Approved Date, Delivered Date) using a single physical calendar table.
* **Outrigger Dimension:** `dim_geolocation` is linked to `dim_customer` and `dim_seller` rather than directly to facts. This normalization reduces address data redundancy.

---

## Project Structure

```bash
.
├── dags/                   # Airflow DAG definitions
│   └── olist_elt_dump_dag.py
├── olist_dbt/              # dbt transformation project
│   ├── models/
│   │   ├── staging/        # Intermediate models (Cleaning, Surrogate Keys)
│   │   └── marts/          # Business models (Galaxy Schema, Ghost Records)
│   ├── seeds/              # Static files (CSV lookups)
│   └── dbt_project.yml
├── scripts/                # Helper scripts
│   ├── run_demo.py         # Simulation orchestrator
│   ├── simulate_production.py
│   └── validate_data.py    # QA Script
├── docker-compose.yml      # Infrastructure definition
├── .sqlfluff               # SQL Linter config
├── requirements.txt        # Python dependencies
└── README.md
```

## Setup & Usage

### Prerequisites
* Docker & Docker Compose
* Python 3.10+

### Quick Start

#### 1. Environment Setup
Copy the example configuration file (contains default passwords for the dev environment).

```bash
# Windows:
copy .env.example .env
# Linux / MacOS:
cp .env.example .env
```

#### 2. Start Infrastructure
Launch Database and Airflow containers.

```bash
docker-compose up -d --build
```

#### 3. Run Simulation (Demo)
The `run_demo.py` script will automatically:
* Create a virtual environment (optional).
* Simulate historical data ingestion (month by month).
* Trigger Airflow DAGs.

```bash
# Prepare Python environment
python -m venv .venv

# Windows:
.\.venv\Scripts\activate
# Linux / MacOS:
source .venv/bin/activate

pip install -r requirements.txt

# Start demo
python scripts/run_demo.py
```

### Roadmap & Status

- [x] Infrastructure: Dockerized Airflow & Postgres.
- [x] ELT Logic: Custom Python Operators with transactional consistency.
- [x] Transformation: dbt Incremental models & Galaxy Schema implementation.
- [x] Orchestration: Event-driven architecture via Airflow API.
- [x] QA: Automatic data validation (validate_data.py).
- [x] Documentation: Hosted dbt docs on GitHub Pages.
- [x] CI/CD: GitHub Actions pipelines.
- [ ] BI: Power BI Dashboards.

<div align="center">

Author: Jakub Major

</div>