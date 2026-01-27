# Milliman LTS Demo (SQL + Data Quality + Marts)

This repo contains a small end-to-end demo of:
- Generating synthetic insurance data (policies, premium payments, claims)
- Loading into Postgres
- Running data-quality checks (flagging bad rows)
- Building analytics marts:
  - policy performance
  - monthly trends

## Repo structure
- `src/` — Python scripts (data generation + ingestion)
- `sql/` — SQL scripts to create staging + marts
- `data_raw/` — generated raw files (CSV/JSON)
- `screenshots/` — query results used as evidence / deliverables
- `outputs/` — any exported results (optional)

## Quickstart
### 1) Start Postgres
```bash
docker compose up -d

## Screenshots (evidence)
### Raw load validation
![Raw counts](screenshots/01_raw_counts.png)

### Data quality summary
![DQ summary](screenshots/02_dq_summary.png)

### Example flagged rows
![Bad claims](screenshots/03_bad_claims_examples.png)
![Bad premium rows](screenshots/04_bad_rows_examples.png)

### Gold marts
![Policy performance](screenshots/05_mart_policy_performance_top_loss.png)
![Monthly trends](screenshots/06_mart_monthly_trends_last12.png)

## SQL
All transformations and mart builds are in:
- `sql/pipeline.sql`


