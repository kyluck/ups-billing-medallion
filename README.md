# UPS Billing Medallion  
### Production-Style Bronze → Silver → Gold Pipeline (Postgres + dbt + Docker)

This project implements a production-style analytics pipeline for ingesting weekly UPS invoice exports into an auditable **Bronze** layer, transforming them into clean and typed **Silver** models, and publishing curated **Gold** marts for spend and charge analysis.

The project is designed to demonstrate real-world data engineering patterns:

- Append-only raw ingestion
- Load audit tracking
- Typed transformation layers
- Analytics-ready marts
- Test-driven modeling with dbt
- Containerized local infrastructure

---

# Project Overview

UPS invoices are delivered as raw Excel exports without column headers.  
This pipeline:

1. Ingests raw invoice rows into Postgres (Bronze)
2. Tracks file identity and load metadata
3. Preserves raw data immutably for auditability
4. Builds typed and curated transformation layers using dbt
5. Publishes Gold analytics tables ready for BI tools (e.g., Metabase)

This mirrors how modern data platforms structure pipelines in production environments.

---

# Architecture

## Bronze Layer (Raw, Immutable)

Purpose: Preserve source truth.

- Stores invoice rows exactly as received
- Append-only design (duplicates preserved intentionally)
- File registry with SHA256 hashing
- Load event tracking (rows read, inserted, status, errors)
- Full replay capability

Key tables:
- `bronze_file_registry`
- `bronze_load_event`
- `bronze_invoice_row`

Design principle: **Never mutate raw data.**

---

## Silver Layer (Typed + Standardized)

Built using dbt.

Purpose:
- Convert raw string fields to correct data types
- Normalize column names
- Add derived flags (e.g., credit detection)
- Prepare structured records for analysis

Examples:
- Numeric casting
- Date parsing
- Charge classification
- Credit identification

Design principle: **Make data trustworthy and consistent.**

---

## Gold Layer (Analytics Marts)

Purpose: Deliver clean, BI-ready tables.

Examples:
- Weekly spend summaries
- Shipment-level cost analysis
- Credit vs charge breakdown
- Charge-type aggregations

All Gold models:
- Built via dbt
- Tested
- Designed for dashboard consumption

Design principle: **Optimize for analysis, not ingestion.**

---

# Tech Stack

- PostgreSQL (Dockerized)
- Python (Bronze ingestion)
- dbt (Silver + Gold transformations)
- Docker Compose (local orchestration)
- Metabase (optional BI layer)

---

# Repository Structure

```
.
├── .venv/                  # Local Python virtual environment (not committed)
├── app/                    # Application / ingestion logic
├── data/                   # Local invoice inputs (gitignored)
├── dbt/                    # dbt project (Silver + Gold models)
├── logs/                   # Local logs (gitignored)
├── outputs/                # Generated outputs / artifacts
├── sql/                    # Bootstrap DDL scripts
├── tools/                  # Utility scripts
├── .env                    # Local environment variables (ignored)
├── .env.example            # Example environment configuration
├── .gitignore
├── db_smoke_test.py        # Postgres connectivity test
├── docker-compose.yml
└── README.md
```

---

# Running Locally

## 1. Configure Environment

Copy the example environment file:

```bash
cp .env.example .env
```

Edit values as needed.

---

## 2. Start Postgres

```bash
docker compose up -d
```

This launches a local Postgres container with persistent volume storage.

---

## 3. Ingest Bronze Data

Run the Python ingestion scripts inside `app/` to load invoice files into the Bronze layer.

Bronze ingestion will:

- Register file metadata
- Record load events
- Insert raw rows append-only
- Track row counts and errors

---

## 4. Build Transformations

```bash
dbt build
```

This will:

- Run Silver models
- Run Gold models
- Execute tests
- Validate data integrity

---

# Data Governance Decisions

This project intentionally follows production-style patterns:

### Append-Only Bronze
Raw rows are never updated or deleted. This ensures:

- Full audit trail
- Historical replay capability
- Transparency

### File Hashing
Each file is SHA256 hashed to:

- Prevent accidental duplicate processing
- Ensure load traceability

### Load Events
Each ingestion run records:

- Start time
- End time
- Status (STARTED / SUCCESS / FAILED)
- Rows read
- Rows inserted
- Error messages

### dbt Testing
Gold models include validation tests to:

- Ensure no unexpected nulls
- Validate business logic assumptions
- Maintain analytical integrity

---

# Example Analytical Use Cases

With the Gold layer, you can analyze:

- Weekly total shipping spend
- Credit frequency and magnitude
- Residential vs commercial cost differences
- Accessorial fee impact
- Shipment-level cost drivers

---

# Why This Project Matters

This is not just an invoice loader.

It demonstrates:

- Medallion architecture
- Separation of ingestion and transformation
- Data lineage discipline
- Repeatable modeling
- Production-style local infrastructure
- Analytics engineering best practices

It reflects how real data teams structure financial ingestion pipelines.

---

# Future Enhancements

Potential improvements:

- CI pipeline to run `dbt build` on pull requests
- Synthetic sample data generator
- Additional Gold marts
- Automated anomaly detection
- Dashboard screenshots
- Deployment to cloud Postgres

---

# License

To be added (recommended: MIT for public portfolio use).

