# Milliman LTS Data Engineering Demo (Postgres + Python + SQL + DQ + Marts)

This repo is a small end-to-end data engineering demo modeled after a life insurance analytics workflow (similar to what a professional services / actuarial-tech team might build).

It demonstrates:

- **Raw data generation** (synthetic life insurance policies, premium payments, and claims)
- **Ingestion into a centralized store** (Postgres running in Docker)
- **Staging layer with explicit Data Quality (DQ) rules** (flag bad records rather than silently dropping them)
- **Analytics marts** for **policy performance** and **monthly trends** (loss ratio, premiums vs claims)
- **Evidence screenshots** of row counts, DQ summaries, and mart outputs

> Note: All data is **synthetic** and created by the generator script in `src/`.

---

## What this project showcases (aligned to the internship posting)

**ETL / data pipelines**
- Generate + ingest multiple data sources and formats (CSV + JSON)
- Load into relational tables with consistent types
- Support repeatable pipeline runs (truncate + reload)

**SQL + data modeling**
- Raw → staging → mart pattern
- Join logic across business entities (policies, payments, claims)
- Aggregate outputs for reporting/analytics (per-policy and monthly)

**Data quality + debugging mindset**
- Records are **not just rejected**—they are **flagged with `dq_issue`** so issues are visible and measurable
- Examples included: negative amounts, missing values, unknown foreign keys, impossible dates

**Documentation / communication**
- Clear structure, reproducible steps, and screenshots that tell a story

---

## Architecture

### Layers
1. **Raw files** (`data_raw/`)
   - `policies.csv`
   - `premium_payments.csv`
   - `claims.json`

2. **Raw tables** (Postgres)
   - `raw_policies`
   - `raw_premium_payments`
   - `raw_claims`

3. **Staging tables (with DQ flags)**  
   These tables clean/standardize raw data and add a `dq_issue` column to identify problems:
   - `stg_policies`
   - `stg_premium_payments`
   - `stg_claims`

4. **Gold marts**  
   - `mart_policy_performance`  
     Aggregated premiums + claims per policy, plus `loss_ratio`
   - `mart_monthly_trends`  
     Monthly premiums vs claims and monthly `loss_ratio`

### Data flow diagram (conceptual)
Raw files → Python ingest → `raw_*` tables → SQL transform/DQ → `stg_*` → SQL marts → `mart_*` → screenshots/exports

---

## Repo structure

- `src/`
  - `generate_data.py` — creates synthetic CSV/JSON data files
  - `ingest.py` — loads raw files into Postgres raw tables
- `sql/`
  - `pipeline.sql` — creates staging tables, runs DQ checks, and builds mart tables
- `docker-compose.yml` — starts Postgres locally in Docker
- `screenshots/` — evidence of outputs and validation queries

---

## Setup / Requirements

### Software
- Docker Desktop
- Python 3.x
- A SQL client (recommended: DBeaver)

### Python dependencies
Installed into your virtual env:
- pandas
- sqlalchemy
- psycopg2-binary
- python-dotenv (optional)
- loguru (optional)

> Optional improvement (recommended): add a `requirements.txt` and install with  
> `pip install -r requirements.txt`

---

## Quickstart (reproduce end-to-end)

### 0) Clone the repo
```bash
git clone https://github.com/MihirGaudani/milliman-lts-demo.git
cd milliman-lts-demo
